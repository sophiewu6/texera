package edu.uci.ics.textdb.exp.regexmatcher;

import jregex.*;

import java.util.*;
import java.util.stream.Collectors;

import com.google.re2j.PublicParser;
import com.google.re2j.PublicRE2;
import com.google.re2j.PublicRegexp;
import com.google.re2j.PublicSimplify;
import com.google.re2j.PublicRegexp.PublicOp;

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


class SpanListSummary{
	public int startMin;
	public int startMax;
	public int endMin;
	public int endMax;
	public float lengthAverage;
	public int size;
	public SpanListSummary(int sMin, int sMax, int eMin, int eMax, float lAvg, int s){
		startMin = sMin;
		startMax = sMax;
		endMin = eMin;
		endMax = eMax;
		lengthAverage = lAvg;
		size = s;
	}
	public SpanListSummary(){
		startMin = -1;
		startMax = -1;
		endMin = -1;
		endMax = -1;
		lengthAverage = -1;
		size = 0;
	}
	// assumes all attributes are the same in the spans of the spanList
	public static SpanListSummary summerize(List<Span> spanList){
		SpanListSummary summary = new SpanListSummary();
		for(Span s: spanList){
			summary.addSpan(s);
		}
		return summary;
	}
	private void addSpan(Span s){
		if(size == 0){
			size = 1;
			startMin = s.getStart();
			startMax = s.getStart();
			endMin = s.getEnd();
			endMax = s.getEnd();
			lengthAverage = s.getEnd() - s.getStart();
		}else{
			startMin = Math.min(startMin, s.getStart());
			startMax = Math.max(startMax, s.getStart());
			endMin = Math.min(endMin, s.getEnd());
			endMax = Math.max(endMax, s.getEnd());
			lengthAverage = (lengthAverage * size + (s.getEnd() - s.getStart())) / (size + 1);
			size ++;
		}
	}
}
class RegexStats{
	int df = 0;
	double tfAverage = 0;
	
	int successDataPointCounter = 0;
	int failureDataPointCounter = 0;
	
	double successCostAverage = 0;
	double failureCostAverage = 0;
	
	double matchingSourceAverageLength = 0;
	
	public void addStatsSubRegexFailure(double failureCost, int matchingSrcSize){
		failureCostAverage = ((failureDataPointCounter * failureCostAverage) + failureCost ) / (failureDataPointCounter + 1);
		failureDataPointCounter ++;
		addStatsMatchingSrcSize(matchingSrcSize);
	}
	
	public void addStatsSubRegexSuccess(int numberOfMatchSpans, double successCost, int matchingSrcSize){
		successCostAverage = ((successDataPointCounter * successCostAverage) + successCost ) / (successDataPointCounter + 1);
		df ++;
		tfAverage = ((successDataPointCounter * tfAverage) + numberOfMatchSpans) / (successDataPointCounter + 1);
		successDataPointCounter ++;
		addStatsMatchingSrcSize(matchingSrcSize);
	}
	
	private void addStatsMatchingSrcSize(int matchingSrcSize){
		matchingSourceAverageLength = (matchingSourceAverageLength * (failureDataPointCounter + successDataPointCounter - 1)) + matchingSrcSize;
		matchingSourceAverageLength = matchingSourceAverageLength / (failureDataPointCounter + successDataPointCounter);
	}
	
	public double getSelectivity(){
		return (successDataPointCounter * 1.0 / (successDataPointCounter + failureDataPointCounter + 1));
	}
	
	public double getExpectedCost(){
		return (successCostAverage * getSelectivity()) + (failureCostAverage * (1 - getSelectivity()));
	}
	
	// The input cover is a list of SubRegexes that are non-high complexity and 
	// will be computed first to serve as the skeleton of the plan. This function
	// returns the cost which depends on the order elements in cover
	public double estimateExpectedCostOfSkeleton(List<SubRegex> cover){
		if(cover.size() == 0) return Double.MAX_VALUE;
		
		// first subregex is applied on all the string, so we multiple the cost by the avg total length of record.
		double expectedCost = cover.get(0).stats.getExpectedCost() * cover.get(0).stats.matchingSourceAverageLength;
		double aggregatedSelectivity = cover.get(0).stats.getSelectivity();
		// Now, for every sub-regex in the cover, first see about the calculated spans to its left and right.
		for(int i = 1; i < cover.size(); ++i){
			SubRegex currentSubRegex = cover.get(i);
			
			// Find the closest computed sub-regex to the left
			SubRegex leftSub = RegexMatcher.findSubRegexNearestComputedLeftNeighbor(cover, i);
			
			// Find the closest computed sub-regex to the right
			SubRegex rightSub = RegexMatcher.findSubRegexNearestComputedRightNeighbor(cover, i);
			
			// Now, execution strategy depends on leftSub and rightSub
			// Number of expected look-ups if we evaluate from left
			// One look-up means finding all the matches starting from one index in the input
			double lookUpSizeFromLeft = currentSubRegex.stats.matchingSourceAverageLength;
			if(leftSub != null && leftSub.getEnd() == currentSubRegex.getStart()){
				lookUpSizeFromLeft = leftSub.stats.tfAverage;
			}
			// And number of expected look-ups if we evaluate from right
			double lookUpSizeFromRight = currentSubRegex.stats.matchingSourceAverageLength;
			if(rightSub != null && rightSub.getStart() == currentSubRegex.getEnd()){
				lookUpSizeFromRight = rightSub.stats.tfAverage;
			}
			
			expectedCost += aggregatedSelectivity * cover.get(i).stats.getExpectedCost() * Math.min(lookUpSizeFromLeft, lookUpSizeFromRight);
			aggregatedSelectivity = aggregatedSelectivity * cover.get(i).stats.getSelectivity();
		}
		return expectedCost;
	}
}


class SubRegex{
	public enum ComplexityLevel{
		High,
		Medium,
		Low
	}
	Pattern regexPatern;
	Pattern startWithRegexPattern;
	Pattern startToEndRegexPattern;
	RegexPredicate regex;
	ComplexityLevel complexity;
	SubRegex reverseSubRegex;
	
	// CSR = CoreSubRegex
	int startingCoreSubRegIndex;
	int numberOfCoreSubRegexes;
	public int getStart(){
		return startingCoreSubRegIndex;
	}
	public int getEnd(){
		return startingCoreSubRegIndex + numberOfCoreSubRegexes;
	}
	
	RegexStats stats = new RegexStats();
	
	// spans belong to the matches in the latest tuple
	List<Span> latestMatchingSpans = null;
	
	public SubRegex(){
		regexPatern = null;
		startWithRegexPattern = null;
		startToEndRegexPattern = null;
		startingCoreSubRegIndex = numberOfCoreSubRegexes = -1;
		this.complexity = ComplexityLevel.Low;
		reverseSubRegex = null;
	}
	public SubRegex(RegexPredicate predicate, int startingCSRIndex, int numberOfCSRs, ComplexityLevel complexity){
		regexPatern = new Pattern(predicate.getRegex());
		startWithRegexPattern = new Pattern("^" + predicate.getRegex());
		startToEndRegexPattern = new Pattern("^" + predicate.getRegex() + "$");
		regex = predicate;
		startingCoreSubRegIndex = startingCSRIndex;
		numberOfCoreSubRegexes = numberOfCSRs;
		this.complexity = complexity;
		reverseSubRegex = null;
	}
	
	public void resetMatchingSpanList(List<Span> spanList){
		latestMatchingSpans = spanList;
	}
	
	public List<Span> getLatestMatchingSpanList(){
		return latestMatchingSpans;
	}
	
	public void setReverseSubRegex(SubRegex reverse){
		reverseSubRegex = reverse;
	}
	
	public SubRegex getReverseSubRegex(){
		return reverseSubRegex;
	}
	
	public String toString(){
		return "[" + startingCoreSubRegIndex + "," + (startingCoreSubRegIndex + numberOfCoreSubRegexes) + ")///" + 
					complexity + "///" + 
					regex.getRegex();
	
	}
	
}
/**
 * Created by chenli on 3/25/16.
 *
 * @author Shuying Lai (laisycs)
 * @author Zuozhi Wang (zuozhiw)
 */
public class RegexMatcher extends AbstractSingleInputOperator {

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
    // pattern not null and used only if regex is not breakable and should always run in one part.
    private Pattern pattern = null;
    
    
    private Schema inputSchema;
    private static int inputTuplesCounter = 0;

    List<SubRegex> coreSubRegexes = new ArrayList<>();
    List<List<SubRegex>> subRegexContainer = new ArrayList<>();
    List<SubRegex> selectedSubRegexes = null;
    
    
    
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
            if(coreSubRegexes.size() <= 1){
            	pattern = new Pattern(predicate.getRegex());
            }else{
            	generateExpandedSubRegexes();
            	// If There is one High complexity sub-regex which covers everything, it's non-breakable
            	if(subRegexContainer.get(0).get(0).numberOfCoreSubRegexes == coreSubRegexes.size()){
                	pattern = new Pattern(predicate.getRegex());
            	}
            	
            }
            // 3. This will continue with collecting/using stats in processOneInputTuple.
            
        
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
        Matcher labelMatcher = new Pattern(CHECK_REGEX_LABEL).matcher(predicate.getRegex());
        if (! labelMatcher.find()) {
            regexType = RegexType.NO_LABELS;
            return;
        }
        Matcher qualifierMatcher = new Pattern(CHECK_REGEX_QUALIFIER).matcher(predicate.getRegex());
        if (qualifierMatcher.find()) {
            Matcher qualifierLabel = new Pattern(CHECK_LABEl_QUALIFIER).matcher(predicate.getRegex());
            Matcher qualifierAffix = new Pattern(CHECK_AFFIX_QUALIFIER).matcher(predicate.getRegex());
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
        	// If there is only one sub-regex or all sub-regexes are High complexity so they merge to one
        	if(pattern != null){
        		matchingResults = computeMatchingResultsWithCompletePattern(inputTuple, predicate, pattern);
                // matchingResults is ready. Will save it in the tuple.
                if (matchingResults.isEmpty()) {
                    return null;
                }

                ListField<Span> spanListField = inputTuple.getField(predicate.getSpanListName());
                List<Span> spanList = spanListField.getValue();
                spanList.addAll(matchingResults);

                return inputTuple;
        	}
        	if(inputTuplesCounter < MAX_TUPLES_FOR_STAT_COLLECTION){
        		// Collecting statistics for each sub regex
        		inputTuplesCounter ++;
        		
        		for(List<SubRegex> subRegexes: subRegexContainer){
        			for(SubRegex sub : subRegexes){
        				// first clean the data structures from the previous tuples
        				sub.resetMatchingSpanList(null);
        				if(sub.complexity == SubRegex.ComplexityLevel.High){
        					// SubRegex has * or +, so in stats collection we only 
        					// check if there is any match to measure selectivity. Cost is always set to zero for these regexes.
        					boolean hasMatch = hasAnyMatches(inputTuple, sub);
        					if(hasMatch){
        						sub.stats.addStatsSubRegexSuccess(1, 0, 0);
        					}else{
        						sub.stats.addStatsSubRegexFailure(0, 0);
        					}
        				}else{
        					// SubRegex doesn't have * or +, so we will find all matches to also measure tf_average
            				long startMatchingTime = System.nanoTime();
            				List<Span> subRegexSpans = computeAllMatchingResults(inputTuple, sub);
            				long endMatchingTime = System.nanoTime();
            				long matchingTime = endMatchingTime - startMatchingTime;
            				int totalTupleTextSize = 1;
            				for(String attributeName : sub.regex.getAttributeNames()){
            		            totalTupleTextSize += inputTuple.getField(attributeName).getValue().toString().length();
            				}
            				double matchingCost = (matchingTime * 1.0) / totalTupleTextSize;
            				if(subRegexSpans == null || subRegexSpans.isEmpty()){
            					// no matches
            					// update statistics with the cost of failure
            					sub.stats.addStatsSubRegexFailure(matchingCost, totalTupleTextSize);
            				}else{
            					// some matches exist
            					// update statistics upon success (df, tf, and success cost)
                				sub.stats.addStatsSubRegexSuccess(subRegexSpans.size(), matchingCost, totalTupleTextSize);
                				// also save the span list for this subregex to be used in the second phase of the algorithm.
                				sub.resetMatchingSpanList(subRegexSpans);
            				}
        				}
        			}
        		}
        		
        		// temporary execution plan for when we are still collecting statistics
        		if(selectedSubRegexes == null){
        			selectedSubRegexes = new ArrayList<>();
        			// first take the non-high complexity SubRegexes
        			int startingCoreIndex = 0;
        			while(startingCoreIndex < subRegexContainer.size()){
        				// Last regex is the longest one for each starting point
        				SubRegex next = subRegexContainer.get(startingCoreIndex).get(subRegexContainer.get(startingCoreIndex).size() - 1);
        				if(next.complexity != SubRegex.ComplexityLevel.High){
        					selectedSubRegexes.add(next);
        				}
        				startingCoreIndex = next.startingCoreSubRegIndex + next.numberOfCoreSubRegexes;
        			}
        			// now, take the high-complexity SubRegexes and put them at the end
        			startingCoreIndex = 0;
        			while(startingCoreIndex < subRegexContainer.size()){
        				// Last regex is the longest one for each starting point
        				SubRegex next = subRegexContainer.get(startingCoreIndex).get(0);
        				if(next.complexity == SubRegex.ComplexityLevel.High){
        					selectedSubRegexes.add(next);
        				}
        				startingCoreIndex = next.startingCoreSubRegIndex + next.numberOfCoreSubRegexes;
        			}
        		}
        	}else if(inputTuplesCounter == MAX_TUPLES_FOR_STAT_COLLECTION){
        		// It's time to use the collected statistics to chose a plan
        		inputTuplesCounter ++;
        		// find a list of subRegexes that together as a plan minimize the expected cost and save it in selectedSubRegexes
        		selectedSubRegexes.clear();
        		findCheapestCoveringListOfSubRegexes();
        		for(SubRegex sub: selectedSubRegexes){
        			sub.resetMatchingSpanList(null);
        		}
        	}else { // inputTuplesCounter > MAX_TUPLES_FOR_STAT_COLLECTION
        		inputTuplesCounter ++;
        		for(SubRegex sub: selectedSubRegexes){
        			sub.resetMatchingSpanList(null);
        		}
        	}
        	printSelectedSubRegexes();
        	
        	// Now use the generated plan:
        	// 1- First, find the span lists for all sub-regexes and return null if any list is empty
        	// 2- Second, generate all possible bigger matches from the sub-matches
        	
        	// 1-
        	for(int i = 0 ; i < selectedSubRegexes.size(); ++i){
        		SubRegex sub = selectedSubRegexes.get(i);
        		if(sub.getLatestMatchingSpanList() == null){
        			sub.resetMatchingSpanList(computeOrderedSubRegexMatchingSpans(inputTuple, selectedSubRegexes, i, coreSubRegexes.size()));
        		}
        		if(sub.getLatestMatchingSpanList() == null || sub.getLatestMatchingSpanList().isEmpty()){
        			return null;
        		}
        	}
        	
        	// 2-
        	
        	
        	// use the generated matching spans and aggregate them into larger spans
        	matchingResults = new ArrayList<>();
        	int nextSubRegexInOriginalOrder = 0;
        	for(int i = 0 ; i < selectedSubRegexes.size(); ++i){
        		if(selectedSubRegexes.get(i).getStart() == 0){
        			nextSubRegexInOriginalOrder = i;
        		}
        	}
        	matchingResults.addAll(selectedSubRegexes.get(nextSubRegexInOriginalOrder).getLatestMatchingSpanList());
			System.out.println("#" + selectedSubRegexes.get(nextSubRegexInOriginalOrder).regex.getRegex() + "#" + 
					selectedSubRegexes.get(nextSubRegexInOriginalOrder).startingCoreSubRegIndex + " size " + 
					selectedSubRegexes.get(nextSubRegexInOriginalOrder).numberOfCoreSubRegexes);
			// now find the next subregex in original order (whose start numberOfCoreSubRegexes coreSubRegexes away)
			boolean hasNext = false;
			for(int i = 0 ; i < selectedSubRegexes.size(); ++i){
				if(selectedSubRegexes.get(i).getStart() == selectedSubRegexes.get(nextSubRegexInOriginalOrder).getEnd()){
					nextSubRegexInOriginalOrder = i;
					hasNext = true;
				}
			}
        	while(hasNext){
        		
    			System.out.println("#" + selectedSubRegexes.get(nextSubRegexInOriginalOrder).regex.getRegex() + "#" + 
    					selectedSubRegexes.get(nextSubRegexInOriginalOrder).startingCoreSubRegIndex + " size " + 
    					selectedSubRegexes.get(nextSubRegexInOriginalOrder).numberOfCoreSubRegexes);
        		
        		List<Span> newMatchingResults = new ArrayList<>();
        		for(Span s: matchingResults){
        			for(Span innerS : selectedSubRegexes.get(nextSubRegexInOriginalOrder).getLatestMatchingSpanList()){
        				if(s.getEnd() == innerS.getStart() && s.getAttributeName().equals(innerS.getAttributeName())){
        					newMatchingResults.add(new Span(s.getAttributeName(), 
        							s.getStart(), 
        							innerS.getEnd(), 
        							s.getKey() + selectedSubRegexes.get(nextSubRegexInOriginalOrder).regex.getRegex(), 
        							s.getValue() + innerS.getValue()));
        				}
        			}
        		}
        		matchingResults.clear();
        		matchingResults.addAll(newMatchingResults);
        		if(matchingResults.isEmpty()){
        			return null;
        		}
    			// now find the next subregex in original order (whose start numberOfCoreSubRegexes coreSubRegexes away)
    			hasNext = false;
    			for(int i = 0 ; i < selectedSubRegexes.size(); ++i){
    				if(selectedSubRegexes.get(i).getStart() == selectedSubRegexes.get(nextSubRegexInOriginalOrder).getEnd()){
    					nextSubRegexInOriginalOrder = i;
    					hasNext = true;
    				}
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
    
    public List<Span> computeOrderedSubRegexMatchingSpans(Tuple inputTuple, List<SubRegex> subRegexes, int index, int numberOfCoreSubRegexes){
    	if(subRegexes == null || subRegexes.isEmpty()){
    		return new ArrayList<>();
    	}
    	SubRegex currentSubRegex = subRegexes.get(index);
    	SubRegex leftBound = findSubRegexNearestComputedLeftNeighbor(subRegexes, index);
    	SubRegex rightBound = findSubRegexNearestComputedRightNeighbor(subRegexes, index);
    	return computeSubRegexMatchesWithComputedNeighbors(inputTuple, currentSubRegex, leftBound, rightBound, numberOfCoreSubRegexes);
    }
    
    public static SubRegex findSubRegexNearestComputedLeftNeighbor(List<SubRegex> subRegexes, int index){
    	SubRegex currentSubRegex = subRegexes.get(index);
    	// find the right most computed sub-regex to the left of current sub-regex
    	SubRegex leftBound = null;
    	for(int j = 0 ; j < index; j++){
    		SubRegex computedSubRegex = subRegexes.get(j);
    		if(computedSubRegex.getEnd() <= currentSubRegex.getStart()){
    			if(leftBound == null || leftBound.getEnd() < computedSubRegex.getEnd()){
    				leftBound = computedSubRegex;
    			}
    		}
    	}
    	return leftBound;
    }
    
    public static SubRegex findSubRegexNearestComputedRightNeighbor(List<SubRegex> subRegexes, int index){
    	SubRegex currentSubRegex = subRegexes.get(index);
    	// find the right most computed sub-regex to the left of current sub-regex
    	SubRegex rightBound = null;
    	for(int j = 0 ; j < index; j++){
    		SubRegex computedSubRegex = subRegexes.get(j);
    		if(computedSubRegex.getStart() >= currentSubRegex.getEnd()){
    			if(rightBound == null || rightBound.getStart() > computedSubRegex.getStart()){
    				rightBound = computedSubRegex;
    			}
    		}
    	}
    	return rightBound;
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
        	return;
        }else{
        	int subIndex = 0;
        	for(PublicRegexp sub : re.getSubs()){
        		SubRegex.ComplexityLevel level = getRegexComplexity(sub);
        		PublicRegexp reverseSub = PublicRegexp.reverseDeepCopy(sub);
        		
        		RegexPredicate subRegexPredicate = new RegexPredicate(sub.toString(), regex.getAttributeNames(), regex.isIgnoreCase(), regex.getSpanListName() + subIndex);
        		SubRegex coreSubRegex = new SubRegex(subRegexPredicate, subIndex, 1, level);
        		// set the reverse sub regex
        		RegexPredicate reverseSubRegexPredicate = new RegexPredicate(reverseSub.toString(), regex.getAttributeNames(), regex.isIgnoreCase(), regex.getSpanListName() + subIndex);
        		coreSubRegex.setReverseSubRegex(new SubRegex(reverseSubRegexPredicate, subIndex, 1, level));

        		coreSubRegexes.add(coreSubRegex);
//        		System.out.println(sub.toString());
//        		System.out.println(reverseSub.toString());
        		subIndex ++;
        	}
        }
    }
    
    private SubRegex.ComplexityLevel getRegexComplexity(PublicRegexp re){
    	SubRegex.ComplexityLevel level = SubRegex.ComplexityLevel.Low;
    	if(PublicRegexp.hasOp(re, PublicOp.STAR) || PublicRegexp.hasOp(re, PublicOp.PLUS)){
    		level = SubRegex.ComplexityLevel.High;
    		return level;
    	}
    	if(PublicRegexp.hasOp(re, PublicOp.ALTERNATE) ||
    			PublicRegexp.hasOp(re, PublicOp.REPEAT) || 
    			PublicRegexp.hasOp(re, PublicOp.QUEST) ||
				PublicRegexp.hasOp(re, PublicOp.ALTERNATE)){
    		level = SubRegex.ComplexityLevel.Medium;
    	}
    	return level;
    }
    
    private void generateExpandedSubRegexes(){
    	for(int startingCSRIndex = 0 ; startingCSRIndex < coreSubRegexes.size(); ++startingCSRIndex){
    		subRegexContainer.add(new ArrayList<>());
    		
    	}
    	for(int startingCSRIndex = 0 ; startingCSRIndex < coreSubRegexes.size(); ++startingCSRIndex){
    		
    		SubRegex coreSubRegex = coreSubRegexes.get(startingCSRIndex);
    		RegexPredicate coreRegexPredicate = coreSubRegex.regex;
    		RegexPredicate expandingPredicate = new RegexPredicate(coreRegexPredicate.getRegex(), 
    				coreRegexPredicate.getAttributeNames(),
    				coreRegexPredicate.isIgnoreCase(),
    				coreRegexPredicate.getSpanListName()+startingCSRIndex);
    		SubRegex.ComplexityLevel expandingComplexityLevel = coreSubRegex.complexity;
    		RegexPredicate reverseExpandingPredicate = new RegexPredicate(coreSubRegex.getReverseSubRegex().regex);

    		if(expandingComplexityLevel == SubRegex.ComplexityLevel.High){ // keep expanding with High complexities
    			int endingCSRIndex = startingCSRIndex;
    			// expandingPredicate => R1
        		for(; endingCSRIndex < coreSubRegexes.size() && coreSubRegexes.get(endingCSRIndex).complexity == SubRegex.ComplexityLevel.High; ++endingCSRIndex){
        			
        			if(startingCSRIndex != endingCSRIndex){
        				// expandingPredicate => R1R2
        				expandingPredicate.setRegex(expandingPredicate.getRegex() + coreSubRegexes.get(endingCSRIndex).regex.getRegex());
        				expandingPredicate.setSpanListName(expandingPredicate.getSpanListName() + endingCSRIndex);
        				// reverseExpandingPredicate => rev(R2)rev(R1)
        				reverseExpandingPredicate.setRegex(coreSubRegexes.get(endingCSRIndex).getReverseSubRegex().regex.getRegex() + reverseExpandingPredicate.getRegex());
        				reverseExpandingPredicate.setSpanListName(reverseExpandingPredicate.getSpanListName() + endingCSRIndex);
        			}
        		}
        		RegexPredicate newSubRegexPredicate = new RegexPredicate(expandingPredicate);
        		SubRegex newSubRegex = new SubRegex(newSubRegexPredicate, startingCSRIndex, endingCSRIndex - startingCSRIndex, expandingComplexityLevel);
        		// Set the reverse sub regex of the new expanded subregex
        		RegexPredicate newReverseSubRegexPredicate = new RegexPredicate(reverseExpandingPredicate);
        		SubRegex newReverseSubRegex = new SubRegex(newReverseSubRegexPredicate, startingCSRIndex, endingCSRIndex - startingCSRIndex, expandingComplexityLevel);
        		newSubRegex.setReverseSubRegex(newReverseSubRegex);
        		subRegexContainer.get(startingCSRIndex).add(newSubRegex);
        		startingCSRIndex = endingCSRIndex - 1;
    		}else{
    			int endingCSRIndex = startingCSRIndex;
    			for(; endingCSRIndex < coreSubRegexes.size() && 
    					coreSubRegexes.get(endingCSRIndex).complexity != SubRegex.ComplexityLevel.High; ++endingCSRIndex){
    				int numberOfCoreSubRegexes = endingCSRIndex - startingCSRIndex + 1;
    				if(startingCSRIndex != endingCSRIndex){
    					// new complecxity level
    					if(expandingComplexityLevel == SubRegex.ComplexityLevel.Medium || coreSubRegexes.get(endingCSRIndex).complexity == SubRegex.ComplexityLevel.Medium){
    						expandingComplexityLevel = SubRegex.ComplexityLevel.Medium;
    					}
    					// expand the regex. R1 => R1R2
    					expandingPredicate.setRegex(expandingPredicate.getRegex() + coreSubRegexes.get(endingCSRIndex).regex.getRegex());
    					expandingPredicate.setSpanListName(expandingPredicate.getSpanListName() + endingCSRIndex);
    					// expand the reverse regex. rev(R1) => rev(R2)rev(R1)
    					reverseExpandingPredicate.setRegex(coreSubRegexes.get(endingCSRIndex).getReverseSubRegex().regex.getRegex() + reverseExpandingPredicate.getRegex());
    					expandingPredicate.setSpanListName(reverseExpandingPredicate.getSpanListName() + endingCSRIndex);
    				}
    				RegexPredicate newSubRegexPredicate = new RegexPredicate(expandingPredicate);
    				RegexPredicate newReverseSubRegexPredicate = new RegexPredicate(reverseExpandingPredicate);
    				SubRegex newSubRegex = new SubRegex(newSubRegexPredicate, startingCSRIndex, numberOfCoreSubRegexes, expandingComplexityLevel);
    				// set the reverse sub regex
    				SubRegex newReverseSubRegex = new SubRegex(newReverseSubRegexPredicate, startingCSRIndex, numberOfCoreSubRegexes, expandingComplexityLevel);
    				newSubRegex.setReverseSubRegex(newReverseSubRegex);
    				subRegexContainer.get(startingCSRIndex).add(newSubRegex);
    			}
    		}
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
    	selectedSubRegexes.addAll(chosenCover.subRegexes);
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
    			estimateOptimumOrderOfCover(partialCoverClone, completeCoverReOrdered);
    			coverCosts.add(completeCoverReOrdered);
    			return;
    		}
    		// continue expanding the partial cover
    		collectPlanCostEstimationsStartingAt(startingCSR + subRegex.numberOfCoreSubRegexes, partialCoverClone, coverCosts);
    	}
    }
    
    // finds the best order of matching of the sub regexes in the given cover. 
    // outputs the corresponding order in the input optimumReorderedCover input.
    private void estimateOptimumOrderOfCover(List<SubRegex> cover, SubRegexCover optimumReorderedCover){
    	// TODO: iterate on different orders of 'cover'
    	// Example: 'H1H2H3''R4R5''H6''R7'
    	// The list of High complexity sub-regexes that are always placed at the end 
    	// of the regex (from smallest to largest selectivity)
    	// in the example above they are 'H1H2H3' and 'H6'
    	List<SubRegex> highComplexitySubs = cover.stream()
    			.filter(
    					s -> 
    						(s.complexity == SubRegex.ComplexityLevel.High)
    					)
    			.sorted(
    					(s1, s2) -> 
    					((Double)(s1.stats.getSelectivity())).compareTo(((Double)(s2.stats.getSelectivity())))
    					)
    			.collect(Collectors.toList());
    	// permutation of non-high complexity sub-regexes.
    	List<List<Integer>> permutations = new ArrayList<>();
    	Set<Integer> subRegexIndexes = new HashSet<>();
    	for(int i =0 ; i < cover.size(); ++i){
    		if(cover.get(i).complexity != SubRegex.ComplexityLevel.High){
    			subRegexIndexes.add(i);
    		}
    	}
    	// generate all the permutations
    	generateAllPermutations(subRegexIndexes, new ArrayList<Integer>(), permutations);
    	
    	// for each permutation estimate the expected cost
    	for(List<Integer> permutation: permutations){
    		// could be 'R7''R4R5'
    		List<SubRegex> reOrderedSkeleton = new ArrayList<>();
    		for(Integer i : permutation){
    			reOrderedSkeleton.add(cover.get(i));
    		}
    		// get the cost
    		double reOrderedSkeletonCost = cover.get(0).stats.estimateExpectedCostOfSkeleton(reOrderedSkeleton);
    		if(optimumReorderedCover.expectedCost == -1 || optimumReorderedCover.expectedCost > reOrderedSkeletonCost){
    			optimumReorderedCover.subRegexes = reOrderedSkeleton;
    			optimumReorderedCover.expectedCost = reOrderedSkeletonCost;
    		}
    	}
    	optimumReorderedCover.subRegexes.addAll(highComplexitySubs);
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
    
    
    private void printAllSubRegexes(){
    	System.out.println("Core SubRegexes:");
    	for(SubRegex core: coreSubRegexes){
    		System.out.println(core.toString());
    		System.out.println(core.getReverseSubRegex().toString());
    	}
    	System.out.println("-----------------------------------------------------------------------------------------------------");
    	for(int i =0 ; i < subRegexContainer.size(); ++i){
    		System.out.println("SubRegexes starting at " + i);
    		for(SubRegex sub: subRegexContainer.get(i)){
    			System.out.println(sub.toString());
    			System.out.println(sub.getReverseSubRegex().toString());
    		}
    		System.out.println("-----------------------------------------------------------------------------------------------------");
    	}
    }
    
    private void printSelectedSubRegexes(){
    	System.out.println("The selected list:");
    	for(SubRegex sub: selectedSubRegexes){
    		System.out.println(sub.toString());
    	}
    	System.out.println("---------------------------------------------------------------");
    }

    public static List<Span> computeMatchingResultsWithCompletePattern(Tuple inputTuple, RegexPredicate predicate, Pattern pattern) {
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
    
    /*
     * Also returns overlapping and nested mathces. More expensive than computeMatchingResultsWithCompletePattern 
     * because it calls proceed() from the RegexMatcher rather than find().
     */
    public static List<Span> computeAllMatchingResults(Tuple inputTuple, SubRegex subRegex){
        List<Span> matchingResults = new ArrayList<>();

        for (String attributeName : subRegex.regex.getAttributeNames()) {
            AttributeType attributeType = inputTuple.getSchema().getAttribute(attributeName).getAttributeType();
            String fieldValue = inputTuple.getField(attributeName).getValue().toString();

            // types other than TEXT and STRING: throw Exception for now
            if (attributeType != AttributeType.STRING && attributeType != AttributeType.TEXT) {
                throw new DataFlowException("KeywordMatcher: Fields other than STRING and TEXT are not supported yet");
            }

            Matcher javaMatcher = subRegex.regexPatern.matcher(fieldValue);
            while (javaMatcher.proceed()) {
                int start = javaMatcher.start();
                int end = javaMatcher.end();
                matchingResults.add(
                        new Span(attributeName, start, end, subRegex.regex.getRegex(), fieldValue.substring(start, end)));
            }
        }

        return matchingResults;
    }
    
    public static List<Span> computeMatchingResultsStartingAt(String attributeName, String src, int start, SubRegex subRegex, boolean findAll){
    	if(start < 0 || start >= src.length()){
    		return new ArrayList<>();
    	}
    	Matcher matcher = subRegex.startWithRegexPattern.matcher(src.substring(start));
    	List<Span> matchingResults = new ArrayList<>();
    	boolean hasMore = findAll? matcher.proceed(): matcher.find();
    	while(hasMore){
    		matchingResults.add(new Span(attributeName, 
    				matcher.start() + start , 
    				matcher.end() + start, 
    				subRegex.regex.getRegex(), 
    				src.substring(matcher.start() + start , matcher.end() + start)));
    		hasMore = findAll? matcher.proceed(): matcher.find();
    	}
    	return matchingResults;
    }
    
    public static boolean doesMatchStartToEnd(String attributeName, String src, int start, int end, SubRegex subRegex){
    	if(start < 0 || start >= src.length() || end < 0 || end >= src.length()){
    		return false;
    	}
    	
    	Matcher matcher = subRegex.startToEndRegexPattern.matcher(src.substring(start, end));
    	return matcher.find();
    }
    
    public static boolean hasAnyMatches(Tuple inputTuple, SubRegex subRegex){
        for (String attributeName : subRegex.regex.getAttributeNames()) {
            AttributeType attributeType = inputTuple.getSchema().getAttribute(attributeName).getAttributeType();
            String fieldValue = inputTuple.getField(attributeName).getValue().toString();

            // types other than TEXT and STRING: throw Exception for now
            if (attributeType != AttributeType.STRING && attributeType != AttributeType.TEXT) {
                throw new DataFlowException("KeywordMatcher: Fields other than STRING and TEXT are not supported yet");
            }

            Matcher javaMatcher = subRegex.regexPatern.matcher(fieldValue);
            if(javaMatcher.find()){
            	return true;
            }
        }
        return false;
    }
    
    public static List<Span> computeSubRegexMatchesWithComputedNeighbors(Tuple inputTuple, 
    		SubRegex subRegex, 
    		SubRegex leftBound, SubRegex rightBound, 
    		int numberOfCoreSubRegexes){
    	
    	List<Span> matchingResults = new ArrayList<>();
    	
    	if(leftBound == null && rightBound == null){ // No computed subRegex is around the subRegex
    		if(subRegex.getStart() == 0 && subRegex.getEnd() == numberOfCoreSubRegexes){
    			return computeMatchingResultsWithCompletePattern(inputTuple, subRegex.regex, subRegex.regexPatern);
    		}else if(subRegex.getStart() == 0){ // subRegex.getEnd() != numberOfCoreSubRegexes
    			// so the right side must be complete.
    	        for (String attributeName : subRegex.regex.getAttributeNames()) {
    	            AttributeType attributeType = inputTuple.getSchema().getAttribute(attributeName).getAttributeType();
    	            String fieldValue = inputTuple.getField(attributeName).getValue().toString();

    	            // types other than TEXT and STRING: throw Exception for now
    	            if (attributeType != AttributeType.STRING && attributeType != AttributeType.TEXT) {
    	                throw new DataFlowException("KeywordMatcher: Fields other than STRING and TEXT are not supported yet");
    	            }
    	            String reverseFieldValue = new StringBuilder(fieldValue).reverse().toString();
    	            for(int i = 0 ; i < fieldValue.length(); ++i){
    	            	List<Span> reverseMatches = computeMatchingResultsStartingAt(attributeName, 
    	            			reverseFieldValue, i, subRegex.getReverseSubRegex(), false);
    	            	for(Span s: reverseMatches){
    	            		matchingResults.add(new Span(attributeName, 
    	            				fieldValue.length() - s.getEnd(),
    	            				fieldValue.length() - s.getStart(), 
    	            				subRegex.regex.getRegex(), 
    	            				fieldValue.substring(fieldValue.length() - s.getEnd(), fieldValue.length() - s.getStart())));
    	            	}
    	            }
    	            
    	        }
    		}else if(subRegex.getEnd() == numberOfCoreSubRegexes){ // && subRegex.getStart() != 0
    			// so the left side must be complete
    	        for (String attributeName : subRegex.regex.getAttributeNames()) {
    	            AttributeType attributeType = inputTuple.getSchema().getAttribute(attributeName).getAttributeType();
    	            String fieldValue = inputTuple.getField(attributeName).getValue().toString();

    	            // types other than TEXT and STRING: throw Exception for now
    	            if (attributeType != AttributeType.STRING && attributeType != AttributeType.TEXT) {
    	                throw new DataFlowException("KeywordMatcher: Fields other than STRING and TEXT are not supported yet");
    	            }
    	            for(int i = 0 ; i < fieldValue.length(); ++i){
    	            	List<Span> spans = computeMatchingResultsStartingAt(attributeName, fieldValue, i, subRegex, false);
    	            	matchingResults.addAll(spans);
    	            }
    	        }
    		}else{ // subRegex.getEnd() != numberOfCoreSubRegexes && subRegex.getStart() != 0
    			// both left and right must be complete.
    			matchingResults.addAll(computeAllMatchingResults(inputTuple, subRegex));
    		}
    	}else if(leftBound == null){ // && rightBound != null // one computed subRegex on right
    		List<Span> rightBoundSpans = rightBound.getLatestMatchingSpanList();
    		if(subRegex.getEnd() == rightBound.getStart()){ // Direct right neighbor is computed.
    			// start reverse matching (from right to left) from rightBound span starting points.
    	        for (String attributeName : subRegex.regex.getAttributeNames()) {
    	            AttributeType attributeType = inputTuple.getSchema().getAttribute(attributeName).getAttributeType();
    	            String fieldValue = inputTuple.getField(attributeName).getValue().toString();
    	            // types other than TEXT and STRING: throw Exception for now
    	            if (attributeType != AttributeType.STRING && attributeType != AttributeType.TEXT) {
    	                throw new DataFlowException("KeywordMatcher: Fields other than STRING and TEXT are not supported yet");
    	            }
    	            String reverseFieldValue = new StringBuilder(fieldValue).reverse().toString();
    	            for(Span rightBoundSpan: rightBoundSpans){
    	            	List<Span> reverseMatches = computeMatchingResultsStartingAt(attributeName, reverseFieldValue, 
    	            			fieldValue.length() - rightBoundSpan.getStart(), subRegex.getReverseSubRegex(), subRegex.getStart() != 0);
    	            	for(Span s: reverseMatches){
    	            		matchingResults.add(new Span(attributeName, 
    	            				fieldValue.length() - s.getEnd(), 
    	            				fieldValue.length() - s.getStart(), 
    	            				subRegex.regex.getRegex(), 
    	            				fieldValue.substring(fieldValue.length() - s.getEnd(), fieldValue.length() - s.getStart())));
    	            	}
    	            }
    	        }
    		}else{ // right bound isn't direct.
    			// start reverse matching (from right to left) from rightBound maximum of span starts to the left
    			SpanListSummary rightBoundSummary = SpanListSummary.summerize(rightBoundSpans);
    	        for (String attributeName : subRegex.regex.getAttributeNames()) {
    	            AttributeType attributeType = inputTuple.getSchema().getAttribute(attributeName).getAttributeType();
    	            String fieldValue = inputTuple.getField(attributeName).getValue().toString();
    	            // types other than TEXT and STRING: throw Exception for now
    	            if (attributeType != AttributeType.STRING && attributeType != AttributeType.TEXT) {
    	                throw new DataFlowException("KeywordMatcher: Fields other than STRING and TEXT are not supported yet");
    	            }
    	            String reverseFieldValue = new StringBuilder(fieldValue).reverse().toString();
    	            for(int i = fieldValue.length() - rightBoundSummary.startMax; i < fieldValue.length(); ++i){
    	            	List<Span> reverseMatches = computeMatchingResultsStartingAt(attributeName, reverseFieldValue, 
    	            			i, subRegex.getReverseSubRegex(), subRegex.getStart() != 0);
    	            	for(Span s: reverseMatches){
    	            		matchingResults.add(new Span(attributeName, 
    	            				fieldValue.length() - s.getEnd(), 
    	            				fieldValue.length() - s.getStart(), 
    	            				subRegex.regex.getRegex(), 
    	            				fieldValue.substring(fieldValue.length() - s.getEnd(), fieldValue.length() - s.getStart())));
    	            	}
    	            }
    	        }
    		}
    	}else if(rightBound == null){ // && leftBound != null // one computed subRegex on left
    		List<Span> leftBoundSpans = leftBound.getLatestMatchingSpanList();
    		if(subRegex.getStart() == leftBound.getEnd()){ // Direct left neighbor is computed.
    			// start matching from leftBound span ending points.
    	        for (String attributeName : subRegex.regex.getAttributeNames()) {
    	            AttributeType attributeType = inputTuple.getSchema().getAttribute(attributeName).getAttributeType();
    	            String fieldValue = inputTuple.getField(attributeName).getValue().toString();
    	            // types other than TEXT and STRING: throw Exception for now
    	            if (attributeType != AttributeType.STRING && attributeType != AttributeType.TEXT) {
    	                throw new DataFlowException("KeywordMatcher: Fields other than STRING and TEXT are not supported yet");
    	            }
    	            for(Span leftBoundSpan: leftBoundSpans){
    	            	List<Span> spans = computeMatchingResultsStartingAt(attributeName, fieldValue, 
    	            			leftBoundSpan.getEnd(), subRegex, subRegex.getEnd() != numberOfCoreSubRegexes);
    	            	matchingResults.addAll(spans);
    	            }
    	        }
    		}else{ // left bound isn't direct.
    			// start matching from leftBound minimum of span ends to the right
    			SpanListSummary leftBoundSummary = SpanListSummary.summerize(leftBoundSpans);
    	        for (String attributeName : subRegex.regex.getAttributeNames()) {
    	            AttributeType attributeType = inputTuple.getSchema().getAttribute(attributeName).getAttributeType();
    	            String fieldValue = inputTuple.getField(attributeName).getValue().toString();
    	            // types other than TEXT and STRING: throw Exception for now
    	            if (attributeType != AttributeType.STRING && attributeType != AttributeType.TEXT) {
    	                throw new DataFlowException("KeywordMatcher: Fields other than STRING and TEXT are not supported yet");
    	            }
    	            for(int i = leftBoundSummary.endMin; i < fieldValue.length(); ++i){
    	            	List<Span> spans = computeMatchingResultsStartingAt(attributeName, fieldValue, 
    	            			i, subRegex, subRegex.getEnd() != numberOfCoreSubRegexes);
    	            	matchingResults.addAll(spans);
    	            }
    	        }
    		}
    	}else{ // rightBound != null && leftBound != null// two computed subRegexes on both sides
    		// start matching from the minimum end of the left bound spans to maximum start of the right bound spans
    		List<Span> leftBoundSpans = leftBound.getLatestMatchingSpanList();
    		List<Span> rightBoundSpans = rightBound.getLatestMatchingSpanList();
    		for (String attributeName : subRegex.regex.getAttributeNames()) {
    			AttributeType attributeType = inputTuple.getSchema().getAttribute(attributeName).getAttributeType();
    			String fieldValue = inputTuple.getField(attributeName).getValue().toString();
    			// types other than TEXT and STRING: throw Exception for now
    			if (attributeType != AttributeType.STRING && attributeType != AttributeType.TEXT) {
    				throw new DataFlowException("KeywordMatcher: Fields other than STRING and TEXT are not supported yet");
    			}
    			List<Span> leftSpans = leftBoundSpans.stream().filter(s -> s.getAttributeName().equals(attributeName)).collect(Collectors.toList());
    			SpanListSummary leftBoundSummary = SpanListSummary.summerize(leftSpans);
    			List<Span> rightSpans = rightBoundSpans.stream().filter(s -> s.getAttributeName().equals(attributeName)).collect(Collectors.toList());
    			SpanListSummary rightBoundSummary = SpanListSummary.summerize(rightSpans);
    			if(leftBound.getEnd() == subRegex.getStart() && subRegex.getEnd() == rightBound.getStart()){ // left and right are both direct neighbors
    				for(Span leftSpan: leftSpans){
    					int start = leftSpan.getEnd();
    					for(Span rightSpan: rightSpans){
    						int end = rightSpan.getStart();
    						if(end < start){
    							continue;
    						}
    						if(doesMatchStartToEnd(attributeName, fieldValue, start, end, subRegex)){
    							matchingResults.add(new Span(attributeName, start, end, subRegex.regex.getRegex(), fieldValue.substring(start, end)));
    						}
    					}
    				}
    			}else if(leftBound.getEnd() == subRegex.getStart()){ // only left is direct neighbor
    				for(Span leftSpan: leftSpans){
    					int start = leftSpan.getEnd();
    					if(start >= rightBoundSummary.startMax){
    						continue;
    					}
    					List<Span> spans = computeMatchingResultsStartingAt(attributeName, fieldValue.substring(0, rightBoundSummary.startMax), 
    							start, subRegex, true);
    					matchingResults.addAll(spans);    					
    				}
    			}else if(subRegex.getEnd() == rightBound.getStart()){ // only right is direct neighbor
    				String reverseFieldValue = new StringBuffer(fieldValue).reverse().toString();
    				for(Span rightSpan: rightSpans){
    					int end = rightSpan.getStart();
    					if(end <= leftBoundSummary.endMin){
    						continue;
    					}
    					int reverseStart = fieldValue.length() - end;
    					int reverseEndBound = fieldValue.length() - leftBoundSummary.endMin;
    					List<Span> spans = computeMatchingResultsStartingAt(attributeName, reverseFieldValue.substring(0, reverseEndBound), 
    							reverseStart, subRegex.getReverseSubRegex(), true);
    					for(Span reverseSpan: spans){
    						matchingResults.add(new Span(attributeName, 
    								fieldValue.length() - reverseSpan.getEnd(), 
    								fieldValue.length() - reverseSpan.getStart(), 
    								subRegex.regex.getRegex(), 
    								fieldValue.substring(fieldValue.length() - reverseSpan.getEnd(), fieldValue.length() - reverseSpan.getStart())));
    					}
    				}
    				
    			}else { // no left nor right are direct neighbors
    				String fieldValueSubStr = fieldValue.substring(0, rightBoundSummary.startMax);
    				for(int i = leftBoundSummary.endMin; i < rightBoundSummary.startMax; ++i){
    					List<Span> spans = computeMatchingResultsStartingAt(attributeName, fieldValueSubStr, i, subRegex, true);
    					matchingResults.addAll(spans);
    				}
    				
    			}
    		}
	        
    		
    	}
    	
    	return matchingResults;
    }
    
}