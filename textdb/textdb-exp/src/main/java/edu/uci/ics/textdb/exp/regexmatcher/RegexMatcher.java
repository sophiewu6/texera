package edu.uci.ics.textdb.exp.regexmatcher;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.re2j.PublicParser;
import com.google.re2j.PublicRE2;
import com.google.re2j.PublicRegexp;
import com.google.re2j.PublicSimplify;

import edu.uci.ics.textdb.api.constants.ErrorMessages;
import edu.uci.ics.textdb.api.exception.DataFlowException;
import edu.uci.ics.textdb.api.exception.TextDBException;
import edu.uci.ics.textdb.api.field.ListField;
import edu.uci.ics.textdb.api.schema.Attribute;
import edu.uci.ics.textdb.api.schema.AttributeType;
import edu.uci.ics.textdb.api.schema.Schema;
import edu.uci.ics.textdb.api.span.Span;
import edu.uci.ics.textdb.api.tuple.Tuple;
import edu.uci.ics.textdb.api.utils.Utils;
import edu.uci.ics.textdb.exp.common.AbstractSingleInputOperator;
import edu.uci.ics.textdb.exp.regexmatcher.label.LabeledRegexProcessor;
import edu.uci.ics.textdb.exp.regexmatcher.label.LabledRegexNoQualifierProcessor;
import edu.uci.ics.textdb.exp.utils.DataflowUtils;

import com.google.re2j.PublicParser;
import com.google.re2j.PublicRE2;
import com.google.re2j.PublicRegexp;
import com.google.re2j.PublicSimplify;


/**
 * Created by chenli on 3/25/16.
 *
 * @author Shuying Lai (laisycs)
 * @author Zuozhi Wang (zuozhiw)
 */
public class RegexMatcher extends AbstractSingleInputOperator {

	class RegexStats{
		int df = 0;
		double tfAverage = 0;
		
		int successDataPointCounter = 0;
		int failureDataPointCounter = 0;
		
		double successCostAverage = 0;
		double failureCostAverage = 0;
		
		public void addStatsSubRegexFailure(double failureCost){
			failureCostAverage = ((failureDataPointCounter * failureCostAverage) + failureCost ) / (failureDataPointCounter + 1);
			failureDataPointCounter ++;
		}
		
		public void addStatsSubRegexSuccess(int numberOfMatchSpans, double successCost){
			successCostAverage = ((successDataPointCounter * successCostAverage) + successCost ) / (successDataPointCounter + 1);
			df ++;
			tfAverage = ((successDataPointCounter * tfAverage) + numberOfMatchSpans) / (successDataPointCounter + 1);
			successDataPointCounter ++;
		}
		
		public double getSelectivity(){
			return (successDataPointCounter * 1.0 / (successDataPointCounter + failureDataPointCounter + 1));
		}
		
		public double getExpectedCost(){
			return (successCostAverage * getSelectivity()) + (failureCostAverage * (1 - getSelectivity()));
		}
		
		public double estimateExpectedCostOfCover(List<SubRegex> cover){
			if(cover.size() == 0) return Double.MAX_VALUE;
			
			double expectedCost = cover.get(0).stats.getExpectedCost();
			double aggregatedSelectivity = cover.get(0).stats.getSelectivity();
			for(int i = 1; i < cover.size(); ++i){
				expectedCost += aggregatedSelectivity * cover.get(i).stats.getExpectedCost();
				aggregatedSelectivity = aggregatedSelectivity * cover.get(i).stats.getSelectivity();
			}
			return expectedCost;
		}
	}
	class SubRegex{
		Pattern regexPatern;
		RegexPredicate regex;
		// CSR = CoreSubRegex
		int startingCoreSubRegIndex;
		int numberOfCoreSubRegexes;
		
		RegexStats stats = new RegexStats();
		
		List<Span> latestMatchingSpans = null;
		public SubRegex(){
			regexPatern = null;
			startingCoreSubRegIndex = numberOfCoreSubRegexes = -1;
		}
		public SubRegex(RegexPredicate predicate, int startingCSRIndex, int numberOfCSRs){
			regexPatern = predicate.isIgnoreCase() ? Pattern.compile(predicate.getRegex(), Pattern.CASE_INSENSITIVE) : Pattern.compile(predicate.getRegex());
			regex = predicate;
			startingCoreSubRegIndex = startingCSRIndex;
			numberOfCoreSubRegexes = numberOfCSRs;
		}
		
		public void resetMatchingSpanList(List<Span> spanList){
			latestMatchingSpans = spanList;
		}
		
		public List<Span> getLatestMatchingSpanList(){
			return latestMatchingSpans;
		}
		
	}
	
    public enum RegexType {
        NO_LABELS, LABELED_QUALIFIERS_AFFIX, LABELED_WITH_QUALIFIERS, Labeled_WITHOUT_QUALIFIERS
    }

    /*
     * Regex pattern for determining if the regex has labels.
     * Match "<" in the beginning, and ">" in the end.
     * Between the brackets "<>", there are one or more number of characters,
     *   but cannot be "<" or ">", or the "\" escape character.
     *
     * For example:
     *   "<drug1>": is a label
     *   "<drug\>1": is not a label because the closing bracket is escaped.
     *   "<a <drug> b>" : only the inner <drug> is treated as a label
     *
     * TODO:
     * this regex can't handle escape inside a bracket pair:
     * <a\>b>: the semantic of this regex is, the label itself can be "a>b"
     */
    public static final String CHECK_REGEX_LABEL = "<[^<>\\\\]*>";

    /*
     * Regex pattern for determining if the regex has qualifiers.
     *
     * TODO:
     * this regex doesn't handle qualifiers correct.
     * It only allows alphabets, digits, and backets.
     * But some characters like "_", "-", "=" doesn't have special meaning
     *   and shouldn't be treated as qualifiers.
     */
    public static final String CHECK_REGEX_QUALIFIER = "[^a-zA-Z0-9<> ]";
    public static final String CHECK_LABEl_QUALIFIER = "<[ a-zA-Z0-9]*[^<>a-zA-Z0-9]+";
    public static final String CHECK_AFFIX_QUALIFIER = ">[^a-zA-Z0-9<>]*<";
    public static final int MAX_TUPLES_FOR_STAT_COLLECTION = 1000;

    private final RegexPredicate predicate;
    private RegexType regexType;
    private Pattern pattern;
    
    
    private Schema inputSchema;
    private static int inputTuplesCounter = 0;

    List<SubRegex> coreSubRegexes = new ArrayList<>();
    List<List<SubRegex>> subRegexContainer = new ArrayList<>();
    List<SubRegex> selectedSubRegexes = new ArrayList<>();
    
    LabeledRegexProcessor labeledRegexProcessor;
    LabledRegexNoQualifierProcessor labledRegexNoQualifierProcessor;

    public RegexMatcher(RegexPredicate predicate) {
        this.predicate = predicate;
    }

    @Override
    protected void setUp() throws DataFlowException {
        inputSchema = inputOperator.getOutputSchema();
        outputSchema = inputSchema;

        if (this.inputSchema.containsField(predicate.getSpanListName())) {
            throw new DataFlowException(ErrorMessages.DUPLICATE_ATTRIBUTE(predicate.getSpanListName(), inputSchema));
        }
        outputSchema = Utils.addAttributeToSchema(inputSchema,
                new Attribute(predicate.getSpanListName(), AttributeType.LIST));

        findRegexType();
        // Check if labeled or unlabeled
        if (this.regexType == RegexType.NO_LABELS) {
//        	pattern = predicate.isIgnoreCase() ? Pattern.compile(predicate.getRegex(), Pattern.CASE_INSENSITIVE) : Pattern.compile(predicate.getRegex());

        	
            // set up the needed data structures for optimization and dynamic 
            // evaluation
            // 1. break the regex into a number of sub-regexes that are called CoreSubRegexes or CSRs here.
            breakIntoCoreSubregexes(predicate);
            
            // 2. Expanding the core sub regexes to make compound sub regexes.
            generateExpandedSubRegexes();
            
            // 3. This will continue with stats collecting in the time of stream processing.
        
        } else if (this.regexType == RegexType.LABELED_WITH_QUALIFIERS) {
            labeledRegexProcessor = new LabeledRegexProcessor(predicate);
        } else {
            labledRegexNoQualifierProcessor = new LabledRegexNoQualifierProcessor(predicate, regexType);
        }
    }

    /*
     * Determines the type of the regex: no_label / labeled_with_qualifier / labeled_without_qualifier
     */
    private void findRegexType() {
        Matcher labelMatcher = Pattern.compile(CHECK_REGEX_LABEL).matcher(predicate.getRegex());
        if (! labelMatcher.find()) {
            regexType = RegexType.NO_LABELS;
            return;
        }
        Matcher qualifierMatcher = Pattern.compile(CHECK_REGEX_QUALIFIER).matcher(predicate.getRegex());
        if (qualifierMatcher.find()) {
            Matcher qualifierLabel = Pattern.compile(CHECK_LABEl_QUALIFIER).matcher(predicate.getRegex());
            Matcher qualifierAffix = Pattern.compile(CHECK_AFFIX_QUALIFIER).matcher(predicate.getRegex());
            if(qualifierAffix.find() || qualifierLabel.find()){
                regexType = RegexType.LABELED_WITH_QUALIFIERS;
            }else{
                regexType = RegexType.LABELED_QUALIFIERS_AFFIX;
            }

        } else {
            regexType = RegexType.Labeled_WITHOUT_QUALIFIERS;
        }
    }

    @Override
    protected Tuple computeNextMatchingTuple() throws TextDBException {
        Tuple inputTuple = null;
        Tuple resultTuple = null;

        while ((inputTuple = inputOperator.getNextTuple()) != null) {
            inputTuple = DataflowUtils.getSpanTuple(inputTuple.getFields(), new ArrayList<Span>(), outputSchema);
            resultTuple = processOneInputTuple(inputTuple);
            if (resultTuple != null) {
                break;
            }
        }

        return resultTuple;
    }

    /**
     * This function returns a list of spans in the given tuple that match the
     * regex For example, given tuple ("george watson", "graduate student", 23,
     * "(949)888-8888") and regex "g[^\s]*", this function will return
     * [Span(name, 0, 6, "g[^\s]*", "george watson"), Span(position, 0, 8,
     * "g[^\s]*", "graduate student")]
     *
     * @param inputTuple
     *            document in which search is performed
     * @return a list of spans describing the occurrence of a matching sequence
     *         in the document
     * @throws DataFlowException
     */
    @Override
    public Tuple processOneInputTuple(Tuple inputTuple) throws DataFlowException {
        if (inputTuple == null) {
            return null;
        }

        List<Span> matchingResults;
        if (this.regexType == RegexType.NO_LABELS) {
//        	matchingResults = computeMatchingResultsWithPattern(inputTuple, predicate, pattern);
        	if(inputTuplesCounter < MAX_TUPLES_FOR_STAT_COLLECTION){
        		inputTuplesCounter ++;
        		// all coreSubRegexes must have at least one match
        		boolean coreSubRegexDoesMatch = true;
        		// collect stats
        		// iterate on all sub regexes from small to large, do the matching, and collect statistics.
        		for(List<SubRegex> listOfSubRegs : subRegexContainer){
        			// Note: the lists are sorted already from small to large.
        			for(SubRegex subRegex : listOfSubRegs){
        				// first clean the data structures from the previous tuples
        				subRegex.resetMatchingSpanList(null);
        				// try matching the sub regex with the tuple
        				long startMatchingTime = System.currentTimeMillis();
        				List<Span> subRegexSpans = computeMatchingResultsWithPattern(inputTuple, subRegex.regex, subRegex.regexPatern);
        				long endMatchingTime = System.currentTimeMillis();
        				long matchingTime = endMatchingTime - startMatchingTime;
        				int totalTupleTextSize = 1;
        				for(String attributeName : subRegex.regex.getAttributeNames()){
        		            totalTupleTextSize += inputTuple.getField(attributeName).getValue().toString().length();
        				}
        				double matchingCost = (matchingTime * 1.0) / totalTupleTextSize;
        				if(subRegexSpans == null || subRegexSpans.isEmpty()){
        					// update statistics with the cost of failure
        					subRegex.stats.addStatsSubRegexFailure(matchingCost);
        					
        					// if it's the coreSubRegex that's failing, set the flag to stop 
        					// the outer loop, since nothing else will make a complete match
        					if(subRegex.numberOfCoreSubRegexes == 1){
        						coreSubRegexDoesMatch = false;
        					}
        					continue;
        				}
        				// update statistics upon success (df, tf, and success cost)
        				subRegex.stats.addStatsSubRegexSuccess(subRegexSpans.size(), matchingCost);
        				
        				// also save the span list for this subregex to be used in the second phase of the algorithm.
        				subRegex.resetMatchingSpanList(subRegexSpans);
        			}
        		}

        		// all coreSubRegexes must have at least one match
        		if(! coreSubRegexDoesMatch){
        			return null;
        		}
        		// default selected plan is using all the core sub regexes.
        		selectedSubRegexes.clear();
        		for(List<SubRegex> listOfSubRegs : subRegexContainer){
        			selectedSubRegexes.add(listOfSubRegs.get(0));
        		}
        		// Check if it's time to use the collected statistics to chose a plan
        	}else if(inputTuplesCounter == MAX_TUPLES_FOR_STAT_COLLECTION){
        		inputTuplesCounter ++;
        		// find a list of subRegexes that together as a plan minimize the expected cost.
        		selectedSubRegexes.clear();
        		findCheapestCoveringListOfSubRegexes();
        		// move on the selected list of subRegexes and compute/save the matching spans.
        		for(SubRegex subRegex : selectedSubRegexes){
        			List<Span> subRegexSpans = computeMatchingResultsWithPattern(inputTuple, subRegex.regex, subRegex.regexPatern);
        			if(subRegexSpans == null || subRegexSpans.isEmpty()){
        				return null;
        			}
        			subRegex.resetMatchingSpanList(subRegexSpans);
        		}
        	}else { // inputTuplesCounter > MAX_TUPLES_FOR_STAT_COLLECTION
        		inputTuplesCounter ++;
        		// move on the selected list of subRegexes and compute/save the matching spans.
        		for(SubRegex subRegex : selectedSubRegexes){
        			List<Span> subRegexSpans = computeMatchingResultsWithPattern(inputTuple, subRegex.regex, subRegex.regexPatern);
        			if(subRegexSpans == null || subRegexSpans.isEmpty()){
        				return null;
        			}
        			subRegex.resetMatchingSpanList(subRegexSpans);
        		}
        	}
        	
        	// use the generated matching spans and aggregate them into larger spans
        	matchingResults = new ArrayList<>();
        	matchingResults.addAll(selectedSubRegexes.get(0).getLatestMatchingSpanList());
        	for(int i = 1; i < selectedSubRegexes.size(); ++i){
        		List<Span> newMatchingResults = new ArrayList<>();
        		for(Span s: matchingResults){
        			for(Span innerS : selectedSubRegexes.get(i).getLatestMatchingSpanList()){
        				if(s.getEnd() == innerS.getStart() && s.getAttributeName().equals(innerS.getAttributeName())){
        					newMatchingResults.add(new Span(s.getAttributeName(), 
        							s.getStart(), 
        							innerS.getEnd(), 
        							s.getKey() + selectedSubRegexes.get(i).regex.getRegex(), 
        							s.getValue() + innerS.getValue()));
        				}
        			}
        		}
        		matchingResults.clear();
        		matchingResults.addAll(newMatchingResults);
        		if(matchingResults.isEmpty()){
        			return null;
        		}
        	}
        } else if (this.regexType == RegexType.LABELED_WITH_QUALIFIERS) {
            matchingResults = labeledRegexProcessor.computeMatchingResults(inputTuple);
        } else {
            matchingResults = labledRegexNoQualifierProcessor.computeMatchingResults(inputTuple);
        }

        // matchingResults is ready. Will save it in the tuple.
        if (matchingResults.isEmpty()) {
            return null;
        }

        ListField<Span> spanListField = inputTuple.getField(predicate.getSpanListName());
        List<Span> spanList = spanListField.getValue();
        spanList.addAll(matchingResults);

        return inputTuple;
    }

    public static List<Span> computeMatchingResultsWithPattern(Tuple inputTuple, RegexPredicate predicate, Pattern pattern) {
        List<Span> matchingResults = new ArrayList<>();

        for (String attributeName : predicate.getAttributeNames()) {
            AttributeType attributeType = inputTuple.getSchema().getAttribute(attributeName).getAttributeType();
            String fieldValue = inputTuple.getField(attributeName).getValue().toString();

            // types other than TEXT and STRING: throw Exception for now
            if (attributeType != AttributeType.STRING && attributeType != AttributeType.TEXT) {
                throw new DataFlowException("KeywordMatcher: Fields other than STRING and TEXT are not supported yet");
            }

            Matcher javaMatcher = pattern.matcher(fieldValue);
            while (javaMatcher.find()) {
                int start = javaMatcher.start();
                int end = javaMatcher.end();
                matchingResults.add(
                        new Span(attributeName, start, end, predicate.getRegex(), fieldValue.substring(start, end)));
            }
        }

        return matchingResults;
    }
    
    

    @Override
    protected void cleanUp() throws DataFlowException {
        if (this.regexType == RegexType.NO_LABELS) {
        	System.out.println("No label regex operator closed.");
        } else if (this.regexType == RegexType.LABELED_WITH_QUALIFIERS) {
        	System.out.println("label regex operator closed.");
        } else {
            System.out.println("Total regex matching time is " + labledRegexNoQualifierProcessor.totalMatchingTime);
        	System.out.println("label regex operator closed.");
        }
    }

    public RegexPredicate getPredicate() {
        return this.predicate;
    }
    
    private void breakIntoCoreSubregexes(RegexPredicate regex){
    	PublicRegexp re = PublicParser.parse(regex.getRegex(), PublicRE2.PERL);
        re = PublicSimplify.simplify(re);
        
        if(re.getOp() != PublicRegexp.PublicOp.CONCAT){
        	// non breakable
        }else{
        	int subIndex = 0;
        	for(PublicRegexp sub : re.getSubs()){
        		List<SubRegex> listOfSubRegexes = new ArrayList<RegexMatcher.SubRegex>();
        		
        		RegexPredicate subRegexPredicate = new RegexPredicate(sub.toString(), regex.getAttributeNames(), regex.isIgnoreCase(), regex.getSpanListName() + subIndex);
        		coreSubRegexes.add(new SubRegex(subRegexPredicate, subIndex, 1));
//        		System.out.println(sub.toString());
        		subIndex ++;
        	}
        }
    }
    
    private void generateExpandedSubRegexes(){
    	for(int startingCSRIndex = 0 ; startingCSRIndex < coreSubRegexes.size(); ++startingCSRIndex){
    		List<SubRegex> listOfSubRegexes = new ArrayList<>();
    		
    		RegexPredicate coreRegexPredicate = coreSubRegexes.get(startingCSRIndex).regex;
    		RegexPredicate expandingPredicate = new RegexPredicate(coreRegexPredicate.getRegex(), 
    				coreRegexPredicate.getAttributeNames(),
    				coreRegexPredicate.isIgnoreCase(),
    				coreRegexPredicate.getSpanListName()+startingCSRIndex);
    		
    		for(int endingCSRIndex = startingCSRIndex; endingCSRIndex < coreSubRegexes.size(); ++endingCSRIndex){
    			int numberOfCoreSubRegexes = endingCSRIndex - startingCSRIndex + 1;
    			if(startingCSRIndex != endingCSRIndex){
    				expandingPredicate.setRegex(expandingPredicate.getRegex() + coreSubRegexes.get(endingCSRIndex).regex.getRegex());
    				expandingPredicate.setSpanListName(expandingPredicate.getSpanListName() + endingCSRIndex);
    			}
    			RegexPredicate newSubRegexPredicate = new RegexPredicate(expandingPredicate.getRegex(), 
    					expandingPredicate.getAttributeNames(), 
    					expandingPredicate.isIgnoreCase(), 
    					expandingPredicate.getSpanListName());
    			listOfSubRegexes.add(new SubRegex(newSubRegexPredicate, startingCSRIndex, numberOfCoreSubRegexes));
    		}
    		subRegexContainer.add(listOfSubRegexes);
    	}
    }
    
    private void findCheapestCoveringListOfSubRegexes(){
    	List<SubRegexCover> coverCosts = new ArrayList<>();
    	// compute all possible covers with their costs
    	collectPlanCostEstimationsStartingAt(0, new ArrayList<SubRegex>(), coverCosts);
    	// choose the cover with the smallest expected cost
    	SubRegexCover chosenCover = new SubRegexCover();
    	for(SubRegexCover subRegCover: coverCosts){
    		if(chosenCover.subRegexes == null || chosenCover.expectedCost > subRegCover.expectedCost){
    			chosenCover.subRegexes = subRegCover.subRegexes;
    			chosenCover.expectedCost = subRegCover.expectedCost;
    		}
    	}
    	selectedSubRegexes.clear();
    	for(SubRegex subRegex : chosenCover.subRegexes){
    		selectedSubRegexes.add(subRegex);
    	}
    }
    
    class SubRegexCover{
    	public List<SubRegex> subRegexes = null;
    	public double expectedCost = -1;
    }
    
    private void collectPlanCostEstimationsStartingAt(int startingCSR, List<SubRegex> partialCover, List<SubRegexCover> coverCosts){
    	for(SubRegex subRegex: subRegexContainer.get(startingCSR)){
    		List<SubRegex> partialCoverClone = new ArrayList<>(partialCover);
    		partialCoverClone.add(subRegex);
    		
    		// collect the covers and their costs in the coverCosts argument when we reach the end.
    		if(startingCSR + subRegex.numberOfCoreSubRegexes == subRegexContainer.size()){
    			// partialCoverClone is complete. Estimate the expected cost and save
    			SubRegexCover completeCoverReOrdered = new SubRegexCover();
    			// estimate the expected cost of the cover.
    			estimateMinimumCostOfCoverAndReorderCover(partialCoverClone, completeCoverReOrdered);
    			coverCosts.add(completeCoverReOrdered);
    			return;
    		}
    		// continue expanding the partial cover
    		collectPlanCostEstimationsStartingAt(startingCSR + subRegex.numberOfCoreSubRegexes, partialCoverClone, coverCosts);
    	}
    }
    
    // finds the best order of matching of the sub regexes in the given cover. 
    // outputs the corresponding order in the input optimumReorderedCover input.
    private void estimateMinimumCostOfCoverAndReorderCover(List<SubRegex> cover, SubRegexCover optimumReorderedCover){
    	// TODO: iterate on different orders of 'cover'
    	List<List<Integer>> permutations = new ArrayList<>();
    	Set<Integer> subRegexIndexes = new HashSet<>();
    	for(int i =0 ; i < cover.size(); ++i){
    		subRegexIndexes.add(i);
    	}
    	// generate all the permutations
    	generateAllPermutations(subRegexIndexes, new ArrayList<Integer>(), permutations);
    	
    	// for each permutation estimate the expected cost
    	for(List<Integer> permutation: permutations){
    		List<SubRegex> reOrderedCover = new ArrayList<>();
    		for(Integer i : permutation){
    			reOrderedCover.add(cover.get(i));
    		}
    		// get the cost
    		double reOrderedCoverCost = cover.get(0).stats.estimateExpectedCostOfCover(reOrderedCover);
    		if(optimumReorderedCover.expectedCost == -1 || optimumReorderedCover.expectedCost > reOrderedCoverCost){
    			optimumReorderedCover.subRegexes = reOrderedCover;
    			optimumReorderedCover.expectedCost = reOrderedCoverCost;
    		}
    	}
    	return;
    }
    
    private void generateAllPermutations(Set<Integer> elements, List<Integer> currentPermutation, List<List<Integer>> permutations){
    	if(elements.isEmpty()){
    		permutations.add(new ArrayList<>(currentPermutation));
    		return;
    	}
    	Set<Integer> elementsCopy = new HashSet<>(elements);
    	for(Integer element: elements){
    		// if element is used next
    		currentPermutation.add(element);
    		elementsCopy.remove(element);
    		generateAllPermutations(elementsCopy, currentPermutation, permutations);
    		currentPermutation.remove(currentPermutation.size() - 1);
    		elementsCopy.add(element);
    	}
    	return;
    }
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    

}