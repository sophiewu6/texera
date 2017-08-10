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
	List<Double> tfAveragePerAttribute = new ArrayList<>();
	
	int successDataPointCounter = 0;
	int failureDataPointCounter = 0;
	
	double successCostAverage = 0;
	double failureCostAverage = 0;
	
	double matchingSrcAvgLen = 0;
	
	public RegexStats(int numAttributes){
		for(int i = 0 ;  i < numAttributes; i++){
			tfAveragePerAttribute.add(0.0);
		}
		tfAveragePerAttribute.set(0, 1.0);
		// To avoid absolute zero for selectivity.
		successDataPointCounter++;
	}
	
	public void addStatsSubRegexFailure(double failureCost, int matchingSrcSize){
		failureCostAverage = ((failureDataPointCounter * failureCostAverage) + failureCost ) / (failureDataPointCounter + 1);
		failureDataPointCounter ++;
		addStatsMatchingSrcSize(matchingSrcSize);
	}
	
	public void addStatsSubRegexSuccess(List<Integer> numberOfMatchSpans, double successCost, int matchingSrcSize){
		// update cost
		successCostAverage = ((successDataPointCounter * successCostAverage) + successCost ) / (successDataPointCounter + 1);
		df ++;
		// update the tf values
		if(numberOfMatchSpans.size() == tfAveragePerAttribute.size()){
			for(int i = 0 ; i < tfAveragePerAttribute.size(); ++i){
				tfAveragePerAttribute.set(i, 
						((successDataPointCounter * tfAveragePerAttribute.get(i)) + numberOfMatchSpans.get(i)) / 
						(successDataPointCounter + 1) );
			}
		}else if(! numberOfMatchSpans.isEmpty()){
			System.out.println("tfAverage input not consistent with the number of attributes."); // TODO remove the print
		}
		// update selectivity
		successDataPointCounter ++;
		// update matching src size
		addStatsMatchingSrcSize(matchingSrcSize);
	}
	
	private void addStatsMatchingSrcSize(int matchingSrcSize){
		matchingSrcAvgLen = (matchingSrcAvgLen * Math.max(getSize()-1,0)) + matchingSrcSize;
		matchingSrcAvgLen = matchingSrcAvgLen / Math.max(getSize(), 1);
	}
	
	public double getSelectivity(){
		return (successDataPointCounter * 1.0 / (getSize() + 1));
	}
	
	public double getExpectedCost(){
		return (successCostAverage * getSelectivity()) + (failureCostAverage * (1 - getSelectivity()));
	}
	
	public int getSize(){
		return successDataPointCounter + failureDataPointCounter;
	}
	
	public List<Double> getTfAveragesPerAttributes(){
		return tfAveragePerAttribute;
	}
	
	public double getTotalTfAverages(){
		double totalTfAverage = 0;
		for(double tf : getTfAveragesPerAttributes()){
			totalTfAverage += tf;
		}
		return totalTfAverage;
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
	
	RegexStats stats = null;
	
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
		stats = new RegexStats(regex.getAttributeNames().size());
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
	
	public int getStart(){
		return startingCoreSubRegIndex;
	}
	public int getEnd(){
		return startingCoreSubRegIndex + numberOfCoreSubRegexes;
	}
	
	public String toString(){
		return "[" + getStart() + "," + getEnd() + ")///" + complexity + "///" + regex.getRegex();
	}
	
	public boolean isReverseExecutionFaster(){
		if(getReverseSubRegex() == null){
			return false;
		}
		return getReverseSubRegex().stats.getExpectedCost() < stats.getExpectedCost();
	}
	
	public double getExpectedCost(){
		if(isReverseExecutionFaster()){
			return getReverseSubRegex().stats.getExpectedCost();
		}else{
			return stats.getExpectedCost();
		}
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


    private RegexType regexType;
    // pattern not null and used only if regex is not breakable and should always run in one part.
    private RegexPredicate mainRegexPredicate;
    private SubRegex mainRegex;
    
    private Schema inputSchema;
    private static int inputTuplesCounter = 0;
    private static int stableChangeCounter = 10;
    private static long firstPhaseTime = 0;

    // List of granular sub-regexes.
    List<SubRegex> coreSubRegexes = new ArrayList<>();
    // The main container of all sub-regexes.
    List<List<SubRegex>> subRegexContainer = new ArrayList<>();
    // Parallel map to isSetNonHighCovering. Records the combined selectivity of each set of sub-regexes.
    Map<Set<SubRegex>, RegexStats> setCombinedStats = new HashMap<>();
    // The final execution plan.
    List<SubRegex> selectedSubRegexes = null;
    
    
    
    LabeledRegexProcessor labeledRegexProcessor;
    LabledRegexNoQualifierProcessor labledRegexNoQualifierProcessor;

    public RegexMatcher(RegexPredicate predicate) {
    	mainRegexPredicate = predicate;
    }
    
    @Override
    protected void setUp() throws DataFlowException {
//    	
//    	java.util.regex.Pattern p = java.util.regex.Pattern.compile("ana");
//    	java.util.regex.Matcher m = p.matcher("banana");
//    	if(m.find()){
//    		do{
//    			System.out.println(m.group());
//    		}while(m.find(m.start() + 1));
//    	}
//    	
        inputSchema = inputOperator.getOutputSchema();
        outputSchema = inputSchema;

        if (this.inputSchema.containsField(mainRegexPredicate.getSpanListName())) {
            throw new DataFlowException(ErrorMessages.DUPLICATE_ATTRIBUTE(mainRegexPredicate.getSpanListName(), inputSchema));
        }
        outputSchema = Utils.addAttributeToSchema(inputSchema,
                new Attribute(mainRegexPredicate.getSpanListName(), AttributeType.LIST));

        findRegexType();
        // Check if labeled or unlabeled
        if (this.regexType == this.regexType) {
//        	pattern = predicate.isIgnoreCase() ? Pattern.compile(predicate.getRegex(), Pattern.CASE_INSENSITIVE) : Pattern.compile(predicate.getRegex());

        	inputTuplesCounter = 0;
        	
            // set up the needed data structures for optimization and dynamic 
            // evaluation
            // 1. break the regex into a number of sub-regexes that are called CoreSubRegexes or CSRs here.
            breakIntoCoreSubregexes();
            if(coreSubRegexes.size() == 1){
            	System.out.println("The original regex is not breakable."); //TODO remove the print
            }
            // 2. Expanding the core sub regexes to make compound sub regexes.
            generateExpandedSubRegexes();
            
            // 3. Populate the combined set maps (isSetNonHighCovering and combinedSetStats) with all possible
            //    and useful key values (combinedSetStats values to be updated in stats collection phase).
            populateCombinedSetStatsMap();

            // 4. This will continue with collecting/using stats in processOneInputTuple.
            //    Note. Even if the regex is not breakable, we still collect statistics to see 
            //          if it's faster to run the regex in reverse direction.
            
        
        } else if (this.regexType == RegexType.LABELED_WITH_QUALIFIERS) {
            labeledRegexProcessor = new LabeledRegexProcessor(mainRegexPredicate);
        } else {
            labledRegexNoQualifierProcessor = new LabledRegexNoQualifierProcessor(mainRegexPredicate, regexType);
        }
    }

    /*
     * Determines the type of the regex: no_label / labeled_with_qualifier / labeled_without_qualifier
     */
    private void findRegexType() {
        Matcher labelMatcher = new Pattern(CHECK_REGEX_LABEL).matcher(mainRegexPredicate.getRegex());
        if (! labelMatcher.find()) {
            regexType = RegexType.NO_LABELS;
            return;
        }
        Matcher qualifierMatcher = new Pattern(CHECK_REGEX_QUALIFIER).matcher(mainRegexPredicate.getRegex());
        if (qualifierMatcher.find()) {
            Matcher qualifierLabel = new Pattern(CHECK_LABEl_QUALIFIER).matcher(mainRegexPredicate.getRegex());
            Matcher qualifierAffix = new Pattern(CHECK_AFFIX_QUALIFIER).matcher(mainRegexPredicate.getRegex());
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
        List<Span> matchingResults = null;
//        if (this.regexType == RegexType.NO_LABELS) {
    	if (this.regexType == this.regexType) {
    		// start collecting statistics from the first tuple in the stream.
        	if(inputTuplesCounter < MAX_TUPLES_FOR_STAT_COLLECTION){
        		long startTime = System.nanoTime();
        		// Collecting statistics for each sub regex
        		inputTuplesCounter ++;
        		// first collect statistics for non-High sub-regexes
        		for(List<SubRegex> subRegexes: subRegexContainer){
        			for(SubRegex sub : subRegexes){
        				if(sub.complexity == SubRegex.ComplexityLevel.High){
        					continue;
        				}
        				boolean shouldContinueStatsCollection = runAndCollectStatistics(inputTuple, sub);
        				// Check if we will have to continue stats collection for the next tuple.
        				if(! shouldContinueStatsCollection){
        					System.out.println("Stats collection is over on record #" + inputTuplesCounter); //TODO remove print
        					inputTuplesCounter = MAX_TUPLES_FOR_STAT_COLLECTION;
        				}
        			}
        		}
        		// Now collect stats for High sub-regexes
        		for(List<SubRegex> subRegexes: subRegexContainer){
        			for(SubRegex sub : subRegexes){
        				if(sub.complexity != SubRegex.ComplexityLevel.High){
        					continue;
        				}
        				boolean shouldContinueStatsCollection = runAndCollectStatistics(inputTuple, sub);
        				// Check if we will have to continue stats collection for the next tuple.
        				if(! shouldContinueStatsCollection){
        					System.out.println("Stats collection is over on record #" + inputTuplesCounter); //TODO remove print
        					inputTuplesCounter = MAX_TUPLES_FOR_STAT_COLLECTION;
        				}
        			}
        		}
        		// Now update selectivity stats for combinedSets
        		updateCombinedSetSelectivity();
        		//////////////////////////////////////////////////// TODO TODO TODO TODO TODO
        		// Temporarily, we set the execution plan to be the original regex running as a whole.
        		if(selectedSubRegexes == null){
        			selectedSubRegexes = new ArrayList<>();
        			selectedSubRegexes.add(mainRegex);
        			if(subRegexContainer.get(0).get(subRegexContainer.get(0).size() - 1) != mainRegex){
        				System.out.println("Unexpected state. Something wrong.");// TODO remove print if this doesn't happen
        			}
        		}
        		long endTime = System.nanoTime();
        		firstPhaseTime += (endTime - startTime);
        	}else if(inputTuplesCounter == MAX_TUPLES_FOR_STAT_COLLECTION){
    			long startTime = System.nanoTime();
        		// It's time to use the collected statistics to chose a plan
        		inputTuplesCounter ++;
        		// find a list of subRegexes that together as a plan minimize the expected cost and save it in selectedSubRegexes
        		selectedSubRegexes.clear();
        		findCheapestCoveringListOfSubRegexes();
        		for(SubRegex sub: selectedSubRegexes){
        			sub.resetMatchingSpanList(null);
        		}
        		long endTime = System.nanoTime();
        		firstPhaseTime += (endTime - startTime);
        		printSelectedSubRegexes();
        	}else { // inputTuplesCounter > MAX_TUPLES_FOR_STAT_COLLECTION
        		inputTuplesCounter ++;
        		for(SubRegex sub: selectedSubRegexes){
        			sub.resetMatchingSpanList(null);
        		}
        	}
//    		if(inputTuplesCounter % 1000 == 0){
//    			System.out.println("Processing the " + inputTuplesCounter + "th tuple ...");
//    		}
        	// Now use the generated plan:
        	// 1- First, find the span lists for all sub-regexes and return null if any list is empty
        	// 2- Second, generate all possible bigger matches from the sub-matches
        	
        	// 1-
        	for(int i = 0 ; i < selectedSubRegexes.size(); ++i){
        		SubRegex sub = selectedSubRegexes.get(i);
        		if(sub.getLatestMatchingSpanList() == null){
        			runSubRegex(inputTuple, i);
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
        			break;
        		}
        	}
        	matchingResults.addAll(selectedSubRegexes.get(nextSubRegexInOriginalOrder).getLatestMatchingSpanList());
//			System.out.println("#" + selectedSubRegexes.get(nextSubRegexInOriginalOrder).regex.getRegex() + "#" + 
//					selectedSubRegexes.get(nextSubRegexInOriginalOrder).startingCoreSubRegIndex + " size " + 
//					selectedSubRegexes.get(nextSubRegexInOriginalOrder).numberOfCoreSubRegexes);
			// now find the next subregex in original order (whose start numberOfCoreSubRegexes coreSubRegexes away)
			boolean hasNext = false;
			for(int i = 0 ; i < selectedSubRegexes.size(); ++i){
				if(selectedSubRegexes.get(i).getStart() == selectedSubRegexes.get(nextSubRegexInOriginalOrder).getEnd()){
					nextSubRegexInOriginalOrder = i;
					hasNext = true;
					break;
				}
			}
        	while(hasNext){
        		
//    			System.out.println("#" + selectedSubRegexes.get(nextSubRegexInOriginalOrder).regex.getRegex() + "#" + 
//    					selectedSubRegexes.get(nextSubRegexInOriginalOrder).startingCoreSubRegIndex + " size " + 
//    					selectedSubRegexes.get(nextSubRegexInOriginalOrder).numberOfCoreSubRegexes);
        		
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
    					break;
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

        ListField<Span> spanListField = inputTuple.getField(mainRegexPredicate.getSpanListName());
        List<Span> spanList = spanListField.getValue();
        spanList.addAll(matchingResults);

        return inputTuple;
    }
    
    public void runSubRegex(Tuple inputTuple, int subRegexIndex){
    	SubRegex sub = selectedSubRegexes.get(subRegexIndex);
		// first clean the data structures from the previous tuples
		sub.resetMatchingSpanList(null);
		List<Span> subRegexSpans = null;
		if(sub.getStart() == 0 && sub.getEnd() == coreSubRegexes.size()){
			if(sub.isReverseExecutionFaster()){
				subRegexSpans = 
						computeMatchingResultsWithCompletePattern(inputTuple, 
								sub.regex, sub.regexPatern, true, 
								sub.getReverseSubRegex().regex, sub.getReverseSubRegex().regexPatern);
			}else{
				subRegexSpans = 
						computeMatchingResultsWithCompletePattern(inputTuple, 
								sub.regex, sub.regexPatern, false, null, null);
			}
			// Save the span list for this subregex to be used in the second phase of the algorithm.
			sub.resetMatchingSpanList(subRegexSpans);
			return;
		}
		
		if(sub.complexity == SubRegex.ComplexityLevel.High){ // High sub-regex
			SubRegex leftBound = findSubRegexNearestComputedLeftNeighbor(selectedSubRegexes, subRegexIndex);
			SubRegex rightBound = findSubRegexNearestComputedRightNeighbor(selectedSubRegexes, subRegexIndex);
			subRegexSpans = 
					computeSubRegexMatchesWithComputedNeighbors(inputTuple, sub, 
							leftBound, rightBound, coreSubRegexes.size(), false, false); // no force on execution direction
			// Save the span list for this subregex to be used in the second phase of the algorithm.
			sub.resetMatchingSpanList(subRegexSpans);
			return;
		}else { // Non-high sub-regex
			subRegexSpans = computeAllMatchingResults(inputTuple, sub, false, false); // no force on execution direction
			// also save the span list for this subregex to be used in the second phase of the algorithm.
			sub.resetMatchingSpanList(subRegexSpans);
			return;
		}
    }
    
	public boolean runAndCollectStatistics(Tuple inputTuple, SubRegex sub){
		// first clean the data structures from the previous tuples
		sub.resetMatchingSpanList(null);
		// If it's the original complete regex, we can collect more precise statistics
		if(sub.getStart() == 0 && sub.getEnd() == coreSubRegexes.size()){
			// left to right execution (normal)
			{
				double oldExpectedCost = sub.stats.getExpectedCost();
				
				long startMatchingTime = System.nanoTime();
				List<Span> subRegexSpans = 
						computeMatchingResultsWithCompletePattern(inputTuple, 
								sub.regex, sub.regexPatern, false, null, null);
				long endMatchingTime = System.nanoTime();
				long matchingTime = endMatchingTime - startMatchingTime;
				int totalTupleTextSize = 1;
				for(String attributeName : sub.regex.getAttributeNames()){
		            totalTupleTextSize += inputTuple.getField(attributeName).getValue().toString().length();
				}
				double matchingCost = (matchingTime * 1.0) / totalTupleTextSize;
				// Save the span list for this subregex to be used in the second phase of the algorithm.
				sub.resetMatchingSpanList(subRegexSpans);
				if(subRegexSpans == null || subRegexSpans.isEmpty()){
					// no matches
					// update statistics with the cost of failure
					sub.stats.addStatsSubRegexFailure(matchingCost, totalTupleTextSize);
				}else{
					// some matches exist
					// update statistics upon success (df, tf, and success cost)
					List<Integer> matchSizesPerAttributes = new ArrayList<>();
					sub.regex.getAttributeNames().stream().
						forEach(attr -> matchSizesPerAttributes.add(
								subRegexSpans.stream().filter(s -> s.getAttributeName().equals(attr)).
														collect(Collectors.toList()).size()
						));
    				sub.stats.addStatsSubRegexSuccess(matchSizesPerAttributes, matchingCost, totalTupleTextSize);
				}
				
				double newExpectedCost = sub.stats.getExpectedCost();
//				if(Math.abs(newExpectedCost - oldExpectedCost) < 1){
//					stableChangeCounter --;
//					if(stableChangeCounter == 0){
//						return false;
//					}
//				}
			}
			// right to left execution (reverse)
			{
				long startMatchingTime = System.nanoTime();
				List<Span> subRegexSpans = 
						computeMatchingResultsWithCompletePattern(inputTuple, 
								sub.regex, sub.regexPatern, true, 
								sub.getReverseSubRegex().regex, sub.getReverseSubRegex().regexPatern);
				long endMatchingTime = System.nanoTime();
				long matchingTime = endMatchingTime - startMatchingTime;
				int totalTupleTextSize = 1;
				for(String attributeName : sub.regex.getAttributeNames()){
		            totalTupleTextSize += inputTuple.getField(attributeName).getValue().toString().length();
				}
				double matchingCost = (matchingTime * 1.0) / totalTupleTextSize;
				// Save the span list for this subregex to be used in the second phase of the algorithm.
				sub.resetMatchingSpanList(subRegexSpans);
				if(subRegexSpans == null || subRegexSpans.isEmpty()){
					// no matches
					// update statistics with the cost of failure
					sub.getReverseSubRegex().stats.addStatsSubRegexFailure(matchingCost, totalTupleTextSize);
				}else{
					// some matches exist
					// update statistics upon success (df, tf, and success cost)
					List<Integer> matchSizesPerAttributes = new ArrayList<>();
					sub.regex.getAttributeNames().stream().
						forEach(attr -> matchSizesPerAttributes.add(
								subRegexSpans.stream().filter(s -> s.getAttributeName().equals(attr)).
														collect(Collectors.toList()).size()
						));
    				sub.getReverseSubRegex().stats.addStatsSubRegexSuccess(matchSizesPerAttributes, matchingCost, totalTupleTextSize);
				}
			}
			return true;
		}
		// If we are here, sub is not the original complete regex.
		if(sub.complexity == SubRegex.ComplexityLevel.High){
			// SubRegex has * or +, so we should collect the stats of verification.
			// The assumption is when we reach to the execution of a High sub-regex, one or
			// both sides of the sub-regex is/are already computed for other sub-regexes.
			SubRegex leftBound = null;
			for(int i = 0 ; i < sub.getStart(); ++i){
				for(int j = subRegexContainer.get(i).size() - 1; j >= 0 ; j--){
					SubRegex possibleLeftBound = subRegexContainer.get(i).get(j);
					if(possibleLeftBound.getEnd() == sub.getStart()){
						leftBound = possibleLeftBound;
						break;
					}else if(possibleLeftBound.getEnd() < sub.getStart()){
						break;
					}
				}
				if(leftBound != null){
					break;
				}
			}
			SubRegex rightBound = null;
			if(sub.getEnd() < subRegexContainer.size()){
				rightBound = subRegexContainer.get(sub.getEnd()).get(subRegexContainer.get(sub.getEnd()).size() - 1);
			}
			{	// measure the time of verification if we use direct order
				long startMatchingTime = System.nanoTime();
				List<Span> subRegexSpans = 
						computeSubRegexMatchesWithComputedNeighbors(inputTuple, sub, 
								leftBound, rightBound, coreSubRegexes.size(), true, false); // force not to use reverse execution
				long endMatchingTime = System.nanoTime();
				long matchingTime = endMatchingTime - startMatchingTime;
				int numVerifications = computeNumberOfNeededVerifications(inputTuple, sub, leftBound, rightBound);
				double verificationCost = (matchingTime * 1.0) / Math.max(numVerifications, 1);
				// Save the span list for this subregex to be used in the second phase of the algorithm.
				sub.resetMatchingSpanList(subRegexSpans);
				if(subRegexSpans == null || subRegexSpans.isEmpty()){
					// no matches
					// update statistics with the cost of failure
					sub.stats.addStatsSubRegexFailure(verificationCost, numVerifications);
				}else{
					// some matches exist
					// update statistics upon success (df, tf, and success cost)
					List<Integer> matchSizesPerAttributes = new ArrayList<>();
					sub.regex.getAttributeNames().stream().
						forEach(attr -> matchSizesPerAttributes.add(
								subRegexSpans.stream().filter(s -> s.getAttributeName().equals(attr)).
														collect(Collectors.toList()).size()
						));
					sub.stats.addStatsSubRegexSuccess(matchSizesPerAttributes, verificationCost, numVerifications);
				}
			}
			{	// measure the time of verification if we use reverse order
				long startMatchingTime = System.nanoTime();
				List<Span> subRegexSpans = 
						computeSubRegexMatchesWithComputedNeighbors(inputTuple, sub, 
								leftBound, rightBound, coreSubRegexes.size(), true, true); // force to use reverse execution
				long endMatchingTime = System.nanoTime();
				long matchingTime = endMatchingTime - startMatchingTime;
				int numVerifications = computeNumberOfNeededVerifications(inputTuple, sub, leftBound, rightBound);
				double verificationCost = (matchingTime * 1.0) / Math.max(numVerifications, 1);
				// Save the span list for this subregex to be used in the second phase of the algorithm.
				sub.resetMatchingSpanList(subRegexSpans);
				if(subRegexSpans == null || subRegexSpans.isEmpty()){
					// no matches
					// update statistics with the cost of failure
					sub.getReverseSubRegex().stats.addStatsSubRegexFailure(verificationCost, numVerifications);
				}else{
					// some matches exist
					// update statistics upon success (df, tf, and success cost)
					List<Integer> matchSizesPerAttributes = new ArrayList<>();
					sub.regex.getAttributeNames().stream().
						forEach(attr -> matchSizesPerAttributes.add(
								subRegexSpans.stream().filter(s -> s.getAttributeName().equals(attr)).
														collect(Collectors.toList()).size()
						));
					sub.getReverseSubRegex().stats.addStatsSubRegexSuccess(matchSizesPerAttributes, verificationCost, numVerifications);
				}
			}
		}else{
			// SubRegex doesn't have * or +, so we will find all matches to also measure tf_average
			// Collect statistics for running normally
			{
				long startMatchingTime = System.nanoTime();
				List<Span> subRegexSpans = computeAllMatchingResults(inputTuple, sub, true, false); // run normally
				long endMatchingTime = System.nanoTime();
				long matchingTime = endMatchingTime - startMatchingTime;
				int totalTupleTextSize = 1;
				for(String attributeName : sub.regex.getAttributeNames()){
					totalTupleTextSize += inputTuple.getField(attributeName).getValue().toString().length();
				}
				double matchingCost = (matchingTime * 1.0) / totalTupleTextSize;
				// also save the span list for this subregex to be used in the second phase of the algorithm.
				sub.resetMatchingSpanList(subRegexSpans);
				if(subRegexSpans == null || subRegexSpans.isEmpty()){
					// no matches
					// update statistics with the cost of failure
					sub.stats.addStatsSubRegexFailure(matchingCost, totalTupleTextSize);
				}else{
					// some matches exist
					// update statistics upon success (df, tf, and success cost)
					List<Integer> matchSizesPerAttributes = new ArrayList<>();
					sub.regex.getAttributeNames().stream().
						forEach(attr -> matchSizesPerAttributes.add(
								subRegexSpans.stream().filter(s -> s.getAttributeName().equals(attr)).
														collect(Collectors.toList()).size()
						));
					sub.stats.addStatsSubRegexSuccess(matchSizesPerAttributes, matchingCost, totalTupleTextSize);
				}
			}
			// Collect statistics for running in reverse
			{
				long startMatchingTime = System.nanoTime();
				List<Span> subRegexSpans = computeAllMatchingResults(inputTuple, sub, true, true); // run in reverse
				long endMatchingTime = System.nanoTime();
				long matchingTime = endMatchingTime - startMatchingTime;
				int totalTupleTextSize = 1;
				for(String attributeName : sub.regex.getAttributeNames()){
					totalTupleTextSize += inputTuple.getField(attributeName).getValue().toString().length();
				}
				double matchingCost = (matchingTime * 1.0) / totalTupleTextSize;
				// also save the span list for this subregex to be used in the second phase of the algorithm.
				sub.resetMatchingSpanList(subRegexSpans);
				if(subRegexSpans == null || subRegexSpans.isEmpty()){
					// no matches
					// update statistics with the cost of failure
					sub.getReverseSubRegex().stats.addStatsSubRegexFailure(matchingCost, totalTupleTextSize);
				}else{
					// some matches exist
					// update statistics upon success (df, tf, and success cost)
					List<Integer> matchSizesPerAttributes = new ArrayList<>();
					sub.regex.getAttributeNames().stream().
						forEach(attr -> matchSizesPerAttributes.add(
								subRegexSpans.stream().filter(s -> s.getAttributeName().equals(attr)).
														collect(Collectors.toList()).size()
						));
					sub.getReverseSubRegex().stats.addStatsSubRegexSuccess(matchSizesPerAttributes, matchingCost, totalTupleTextSize);
				}
			}
		}
		
		return true;
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
//        	System.out.println("No label regex operator closed.");
        } else if (this.regexType == RegexType.LABELED_WITH_QUALIFIERS) {
        	System.out.println("label regex operator closed.");
        } else {
            System.out.println("Total regex matching time is " + labledRegexNoQualifierProcessor.totalMatchingTime);
        	System.out.println("label regex operator closed.");
        }
        System.out.println("First phase time : " + firstPhaseTime * 1.0 / 1000000000);
    }

    public RegexPredicate getPredicate() {
        return mainRegexPredicate;
    }
    
    // If possible, breaks the main regex and populates the coreSubRegexes vector
    // If mainRegexPredicate is not breakble, it puts it entirely as the only subregex in 
    // coreSubRegexes vector.
    private void breakIntoCoreSubregexes(){
    	PublicRegexp re = PublicParser.parse(mainRegexPredicate.getRegex(), PublicRE2.PERL);
        re = PublicSimplify.simplify(re);
        
        int numberOfCoreSubRegexes = 0;
        SubRegex.ComplexityLevel mainRegexComplexity = SubRegex.ComplexityLevel.Low;
        RegexPredicate mainReverseRegexPredicate = 
        		new RegexPredicate("", mainRegexPredicate.getAttributeNames(), 
        				mainRegexPredicate.getSpanListName());
        if(re.getOp() != PublicRegexp.PublicOp.CONCAT){
        	numberOfCoreSubRegexes = 1;
        	mainRegexComplexity = getRegexComplexity(re);
        	mainReverseRegexPredicate.setRegex(PublicRegexp.reverseDeepCopy(re).toString());
        }else{
        	int subIndex = 0;
        	for(PublicRegexp sub : re.getSubs()){
        		SubRegex.ComplexityLevel level = getRegexComplexity(sub);
        		// Keep also calculating the complexity of the full regex
        		if(subIndex == 0){
        			mainRegexComplexity = level;
        		}else{
        			if(level == SubRegex.ComplexityLevel.High){
        				mainRegexComplexity = SubRegex.ComplexityLevel.High;
        			}else if(level == SubRegex.ComplexityLevel.Medium &&
        					mainRegexComplexity != SubRegex.ComplexityLevel.High){
        				mainRegexComplexity = SubRegex.ComplexityLevel.Medium;
        			}
        		}
        		// calculate the reverse of this sub-regex
        		PublicRegexp reverseSub = PublicRegexp.reverseDeepCopy(sub);
        		// continue building the reverse of the complete regex too
        		mainReverseRegexPredicate.setRegex(reverseSub.toString() + mainReverseRegexPredicate.getRegex());
        		// prepare the objects to be stored in coreSubRegexes vector.
        		RegexPredicate subRegexPredicate = new RegexPredicate(sub.toString(),
        				mainRegexPredicate.getAttributeNames(), mainRegexPredicate.isIgnoreCase(), 
        				mainRegexPredicate.getSpanListName() + subIndex);
        		SubRegex coreSubRegex = new SubRegex(subRegexPredicate, subIndex, 1, level);
        		// set the reverse sub regex
        		RegexPredicate reverseSubRegexPredicate = new RegexPredicate(reverseSub.toString(), 
        				mainRegexPredicate.getAttributeNames(), mainRegexPredicate.isIgnoreCase(), 
        				mainRegexPredicate.getSpanListName() + subIndex);
        		coreSubRegex.setReverseSubRegex(new SubRegex(reverseSubRegexPredicate, subIndex, 1, level));

        		coreSubRegexes.add(coreSubRegex);
//        		System.out.println(sub.toString());
//        		System.out.println(reverseSub.toString());
        		subIndex ++;
        	}
        	numberOfCoreSubRegexes = subIndex;
        }
        
        mainRegex = new SubRegex(mainRegexPredicate, 0, numberOfCoreSubRegexes, mainRegexComplexity);
        SubRegex mainReverseRegex = new SubRegex(mainReverseRegexPredicate, 
        		0, numberOfCoreSubRegexes, mainRegexComplexity);
        mainRegex.setReverseSubRegex(mainReverseRegex);
        
        if(coreSubRegexes.isEmpty()){
        	coreSubRegexes.add(mainRegex);
        }
//        System.out.println("---------------------------------------");
//        for(SubRegex subRegex: coreSubRegexes){
//        	System.out.println(subRegex.regex.getRegex());
//        }
//        System.out.println("---------------------------------------");
        
        if(numberOfCoreSubRegexes != coreSubRegexes.size()){
        	System.out.println("Something wrong."); // TODO maybe never happens 
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
    	
    	// initialize the subRegex container
    	for(int startingCSRIndex = 0 ; startingCSRIndex < coreSubRegexes.size(); ++startingCSRIndex){
    		subRegexContainer.add(new ArrayList<>());
    		
    	}
    	// start making new sub-regexes by expanding the core sub-regexes from each starting point.
    	// Note: Non-high and high sub-regexes are not mixed.
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
        		if(newSubRegex.getStart() == 0 && newSubRegex.getEnd() == coreSubRegexes.size()){
        			// this is the full regex, we have previously prepared this regex in the break function
        			subRegexContainer.get(startingCSRIndex).add(mainRegex);
        		}else{
        			RegexPredicate newReverseSubRegexPredicate = new RegexPredicate(reverseExpandingPredicate);
        			SubRegex newReverseSubRegex = new SubRegex(newReverseSubRegexPredicate, startingCSRIndex, endingCSRIndex - startingCSRIndex, expandingComplexityLevel);
        			newSubRegex.setReverseSubRegex(newReverseSubRegex);
        			subRegexContainer.get(startingCSRIndex).add(newSubRegex);
        		}
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
    				if(newSubRegex.getStart() == 0 && newSubRegex.getEnd() == coreSubRegexes.size()){
    					subRegexContainer.get(startingCSRIndex).add(mainRegex);
    				}else{
    					// set the reverse sub regex
    					SubRegex newReverseSubRegex = new SubRegex(newReverseSubRegexPredicate, startingCSRIndex, numberOfCoreSubRegexes, expandingComplexityLevel);
    					newSubRegex.setReverseSubRegex(newReverseSubRegex);
    					subRegexContainer.get(startingCSRIndex).add(newSubRegex);
    				}
    			}
    		}
    	}
    	
    	// Also add a sub-regex starting from zero with full length which would be the 
    	// original complete regex.
    	
    	// Check the longest sub-regex starting from zero
    	SubRegex sub = subRegexContainer.get(0).get(subRegexContainer.get(0).size() - 1);
    	if(! (sub.getStart() == 0 && sub.getEnd() == coreSubRegexes.size())){ // the regex was actually broken.
    		// the large complete regex which starts from zero and goes to the end
    		subRegexContainer.get(0).add(mainRegex);
    	}
    }
    
    // For each set as a key of the setCombinedStats map, it checks the latest matching results of 
    // all its elements to update its selectivity.
    private void updateCombinedSetSelectivity(){
    	for(Map.Entry<Set<SubRegex>, RegexStats> pair: setCombinedStats.entrySet()){
    		boolean isSuccessful = true;
    		for(SubRegex sub: pair.getKey()){
    			if(sub.getLatestMatchingSpanList() == null || sub.getLatestMatchingSpanList().isEmpty()){
    				isSuccessful = false;
    				break;
    			}
    		}
    		// All costs are passed 0 because we just want to store selectivity in this map.
    		if(isSuccessful){
    			pair.getValue().addStatsSubRegexSuccess(new ArrayList<>(), 0, 0);
    		}else{
    			pair.getValue().addStatsSubRegexFailure(0, 0);
    		}
    	}
    }
    
    // generates all the useful sets of sub-regexes as the keys of the 
    // setCombinedStats map, so we can store selectivity statistics in its keys.
    private void populateCombinedSetStatsMap(){
        // if the elements of the key cover all the non-high parts of the regex, the value is true
        // and false otherwise.
        Map<Set<SubRegex>, Boolean> isSetNonHighCovering = new HashMap<>();
        
        // 1. First generate those keys that contain only non-high sub-regexes.
        expandCombinedSetsStartingAt(0, new HashSet<SubRegex>(), true, isSetNonHighCovering, false, null);
        
        // 2. For each key, (a) add itself to the setCombinedStats map, and (b) for each key whose value is true,
        // add its expansions by High sub-regexes to setCombinedStats also.
        for(Map.Entry<Set<SubRegex>, Boolean> pair : isSetNonHighCovering.entrySet()){
        	setCombinedStats.put(pair.getKey(), new RegexStats(coreSubRegexes.get(0).regex.getAttributeNames().size()));
        	if(pair.getValue()){
        		Set<SubRegex> newExpandingCombinedSet = new HashSet<>();
        		newExpandingCombinedSet.addAll(pair.getKey());
        		expandCombinedSetsStartingAt(0, newExpandingCombinedSet, true, null, true, setCombinedStats);
        	}
        }
    }
    
    // isFullyCovering is true when the expandingSet has not missed any non-high part of the regex
    private void expandCombinedSetsStartingAt(int start, Set<SubRegex> expandingSet, 
    		boolean isFullyCovering, Map<Set<SubRegex>, Boolean> isSetNonHighCovering, 
    		boolean highSubRegexMode, Map<Set<SubRegex>, RegexStats > combinedSetStats){
    	if(start == subRegexContainer.size()){ // no more elements to add
    		if(! expandingSet.isEmpty()){
    			if(highSubRegexMode){
    				if(! combinedSetStats.containsKey(expandingSet)){
    					Set<SubRegex> expandingSetClone = new HashSet<>();
    					expandingSetClone.addAll(expandingSet);
    					combinedSetStats.put(expandingSetClone, new RegexStats(coreSubRegexes.get(0).regex.getAttributeNames().size()));
    				}
    			}else{
					Set<SubRegex> expandingSetClone = new HashSet<>();
					expandingSetClone.addAll(expandingSet);
    				isSetNonHighCovering.put(expandingSetClone, isFullyCovering);
    			}
    		}
    		return;
    	}
    	if(highSubRegexMode){ // skipping over High sub-regexes.
    		// if next sub-regex is High, try both cases of having and not having it.
    		if(subRegexContainer.get(start).get(0).complexity == SubRegex.ComplexityLevel.High){
    			SubRegex sub = subRegexContainer.get(start).get(0);
    			// not having it
    			expandCombinedSetsStartingAt(sub.getEnd(), 
    					expandingSet, isFullyCovering, isSetNonHighCovering, highSubRegexMode, combinedSetStats);
    			// having it
    			expandingSet.add(sub);
    			expandCombinedSetsStartingAt(sub.getEnd(), expandingSet, isFullyCovering, isSetNonHighCovering, 
    					highSubRegexMode, combinedSetStats);
    			// fix the expanding set for the up-coming sub-regexes.
    			expandingSet.remove(sub);
    			return;
    		}else{
    			expandCombinedSetsStartingAt(start + 1, 
    					expandingSet, isFullyCovering, isSetNonHighCovering, highSubRegexMode, combinedSetStats);
    		}
    	}else{
    		// if next sub-regex is High, jump over it.
    		if(subRegexContainer.get(start).get(0).complexity == SubRegex.ComplexityLevel.High){
    			expandCombinedSetsStartingAt(subRegexContainer.get(start).get(0).getEnd(), 
    					expandingSet, isFullyCovering, isSetNonHighCovering, 
    					highSubRegexMode, combinedSetStats);
    			return;
    		}
    		// Go over sub-regexes and construct the cases of having and not having them
    		for(int i = 0 ; i < subRegexContainer.get(start).size(); i++){
    			SubRegex sub = subRegexContainer.get(start).get(i);
    			// the case of not having this sub-regex.
    			expandCombinedSetsStartingAt(sub.getEnd(), expandingSet, false, isSetNonHighCovering, 
    					highSubRegexMode, combinedSetStats);
    			// the case of having this sub-regex
    			expandingSet.add(sub);
    			expandCombinedSetsStartingAt(sub.getEnd(), expandingSet, isFullyCovering, isSetNonHighCovering, 
    					highSubRegexMode, combinedSetStats);
    			// fix the expanding set for the up-coming sub-regexes.
    			expandingSet.remove(sub);
    		}
    	}
    }
    
    // Populates the selectedSubRegexes list with a list of sub-regexes that 
    // completely cover all the parts of the original regex.
    private void findCheapestCoveringListOfSubRegexes(){
    	List<SubRegexCover> coverCosts = new ArrayList<>();
    	// compute all possible covers with their costs
    	collectPlanCostEstimationsStartingAt(0, new ArrayList<SubRegex>(), coverCosts);
    	// choose the cover with the smallest expected cost
    	SubRegexCover chosenCover = null;
    	for(SubRegexCover subRegCover: coverCosts){
    		System.out.println(subRegCover);
    		if(chosenCover == null){
    			chosenCover = new SubRegexCover();
    			chosenCover.subRegexes = subRegCover.subRegexes;
    			chosenCover.expectedCost = subRegCover.expectedCost;    			
    		}else if(chosenCover.expectedCost > subRegCover.expectedCost){
    			chosenCover.subRegexes = subRegCover.subRegexes;
    			chosenCover.expectedCost = subRegCover.expectedCost;
    		}
    	}
    	selectedSubRegexes.clear();
    	selectedSubRegexes.addAll(chosenCover.subRegexes);
    }
    
    class SubRegexCover{
    	public List<SubRegex> subRegexes = new ArrayList<>();
    	public double expectedCost = -1;
    	public String toString(){
    		String result = "";
    		for(SubRegex sub: subRegexes){
    			result += "\n" + sub.toString();
    		}
    		result += "\nCost is " + expectedCost;
    		return result;
    	}
    }
    
    private void collectPlanCostEstimationsStartingAt(int startingCSR, 
    		List<SubRegex> partialCover, List<SubRegexCover> coverCosts){
    	for(SubRegex subRegex: subRegexContainer.get(startingCSR)){
    		List<SubRegex> partialCoverClone = new ArrayList<>(partialCover);
    		partialCoverClone.add(subRegex);
    		
    		// If the cover becomes complete with this new sub-regex, collect the cover and its cost.
    		if(startingCSR + subRegex.numberOfCoreSubRegexes == subRegexContainer.size()){
    			// partialCoverClone is complete. Estimate the expected cost and save
    			SubRegexCover completeCoverReOrdered = new SubRegexCover();
    			// estimate the expected cost of the cover.
    			estimateOptimumOrderAndCostOfCover(partialCoverClone, completeCoverReOrdered);
    			coverCosts.add(completeCoverReOrdered);
    			return;
    		}
    		// continue expanding the partial cover
    		collectPlanCostEstimationsStartingAt(startingCSR + subRegex.numberOfCoreSubRegexes, partialCoverClone, coverCosts);
    	}
    }
    
    // finds the best order of matching of the sub regexes in the given cover. 
    // outputs the corresponding order in the input optimumReorderedCover input.
    private void estimateOptimumOrderAndCostOfCover(List<SubRegex> cover, SubRegexCover optimumReorderedCover){
//    	if(cover.size() == 1){ // the original complete regex
//    		SubRegex singleCoveringRegex = cover.get(0);
//    		optimumReorderedCover.subRegexes.add(singleCoveringRegex);
//    		optimumReorderedCover.expectedCost = singleCoveringRegex.getExpectedCost();
//    		return;
//    	}

    	// Example: 'H1H2H3''R4R5''H6''R7'
    	// The list of High complexity sub-regexes that are always placed at the end 
    	// of the regex
    	// in the example above they are 'H1H2H3' and 'H6'

    	// 1. Prepare permutations of non-high complexity sub-regexes.
    	Set<Integer> nonHighSubRegexIndexes = new HashSet<>();
    	Set<Integer> highSubRegexIndexes = new HashSet<>();
    	for(int i =0 ; i < cover.size(); ++i){
    		if(cover.get(i).complexity == SubRegex.ComplexityLevel.High){
    			highSubRegexIndexes.add(i);
    		}else{
    			nonHighSubRegexIndexes.add(i);
    		}
    	}
    	// 1. generate all the permutations of non-high sub-regexes.
    	List<List<Integer>> nonHighPermutations = new ArrayList<>();
    	generateAllPermutations(nonHighSubRegexIndexes, new ArrayList<Integer>(), nonHighPermutations);
    	// 2. generate all the permutations of high sub-regexes.
    	List<List<Integer>> highPermutations = new ArrayList<>();
    	generateAllPermutations(highSubRegexIndexes, new ArrayList<Integer>(), highPermutations);
    	// 3. concat non-high and high permutations to make complete permutations
    	List<List<Integer>> permutations = new ArrayList<>();
    	for(List<Integer> nonHighPerm : nonHighPermutations){
        	for(List<Integer> highPerm : highPermutations){
        		List<Integer> completePerm = new ArrayList<>();
        		completePerm.addAll(nonHighPerm);
        		completePerm.addAll(highPerm);
        		permutations.add(completePerm);
        	}    		
    	}
    	
    	// 4. for each permutation estimate the expected cost
    	for(List<Integer> permutation: permutations){
    		// could be 'R7''R4R5'
    		List<SubRegex> candidatePlan = new ArrayList<>();
    		for(Integer i : permutation){
    			candidatePlan.add(cover.get(i));
    		}
    		// get the cost
    		double candidatePlanCost = estimateExpectedCostOfCandidatePlan(candidatePlan);
    		if(optimumReorderedCover.expectedCost == -1 || optimumReorderedCover.expectedCost > candidatePlanCost){
    			optimumReorderedCover.subRegexes = candidatePlan;
    			optimumReorderedCover.expectedCost = candidatePlanCost;
    		}
    	}
    	return;
    }
    
	// The input cover is a list of SubRegexes that will be computed in the same order to 
    // find the matching results of the original regex.
	public double estimateExpectedCostOfCandidatePlan(List<SubRegex> plan){
		if(plan.size() == 0) {
			System.out.println("Unexpected state. Something wrong. Trying to cost empty candidate plan."); // TODO: remove print
			return Double.MAX_VALUE;
		}
		System.out.println("=============================================================");
		System.out.println("Estimating the cost of new plan");
		
		if(plan.size() == 1){
			SubRegex sub = plan.get(0);
			if(! (sub.getStart() == 0 && sub.getEnd() == coreSubRegexes.size())){
				System.out.println("Unexpected state. Something is wrong. Plan with single sub-regex which is not the main regex.");
				return 0;
			}
			System.out.println("Cost of " + sub.toString());
			System.out.println(sub.getExpectedCost() + " * " + sub.stats.matchingSrcAvgLen + " = " + (sub.getExpectedCost() * sub.stats.matchingSrcAvgLen));
			return sub.getExpectedCost() * sub.stats.matchingSrcAvgLen;
		}
		
		double expectedCost = 0;
		// 1. First estimate the cost of the non-high sub-regexes of the plan (the skeleton)
		Set<SubRegex> pastSubRegexes = new HashSet<>();
		for(int i = 0 ; i < plan.size(); ++i){
			SubRegex nextSubRegex = plan.get(i);
			if(nextSubRegex.complexity == SubRegex.ComplexityLevel.High){
				break;
			}
			if(i == 0){
				System.out.println("Adding cost of " + nextSubRegex.toString());
				System.out.println(nextSubRegex.getExpectedCost() + " * " + nextSubRegex.stats.matchingSrcAvgLen + " = " + 
						(nextSubRegex.getExpectedCost() * nextSubRegex.stats.matchingSrcAvgLen));
				expectedCost = nextSubRegex.getExpectedCost() * nextSubRegex.stats.matchingSrcAvgLen;
			}else{
				if(! setCombinedStats.containsKey(pastSubRegexes)){
					System.out.println("Unexpected state. setCombinedStats doesn't have a combination."); // TODO: remove print.
				}
				System.out.println("Adding cost of " + nextSubRegex.toString());
				System.out.println(setCombinedStats.get(pastSubRegexes).getSelectivity() + " * " + nextSubRegex.getExpectedCost() + " * " + nextSubRegex.stats.matchingSrcAvgLen + " = " + 
						(setCombinedStats.get(pastSubRegexes).getSelectivity() * nextSubRegex.getExpectedCost() * nextSubRegex.stats.matchingSrcAvgLen));
				expectedCost = expectedCost + 
						(
							setCombinedStats.get(pastSubRegexes).getSelectivity() * 
							nextSubRegex.getExpectedCost() * 
							nextSubRegex.stats.matchingSrcAvgLen
						);
			}
			//
			pastSubRegexes.add(nextSubRegex);
		}
		
		// 2. Now, continue estimating the cost by estimating the cost of verification for High sub-regexes.
		for(int i = 0 ; i < plan.size(); ++i){
			SubRegex nextSubRegex = plan.get(i);
			if(nextSubRegex.complexity != SubRegex.ComplexityLevel.High){
				continue;
			}
			// Find the closest computed sub-regex to the left
			SubRegex leftSub = RegexMatcher.findSubRegexNearestComputedLeftNeighbor(plan, i);
			// Find the closest computed sub-regex to the right
			SubRegex rightSub = RegexMatcher.findSubRegexNearestComputedRightNeighbor(plan, i);
			double totalNumberOfVerifications = estimateNumberOfNeededVerifications(nextSubRegex, leftSub, rightSub);
			double amortizedCostOfVerification = nextSubRegex.getExpectedCost();
			if(i == 0){
				System.out.println("Adding cost of verification of " + nextSubRegex.toString());
				System.out.println(amortizedCostOfVerification + " * " + totalNumberOfVerifications + " = " +
							(amortizedCostOfVerification * totalNumberOfVerifications));
				expectedCost = amortizedCostOfVerification * totalNumberOfVerifications;
			}else{
				if(! setCombinedStats.containsKey(pastSubRegexes)){
					System.out.println("Unexpected state. setCombinedStats doesn't have a combination."); // TODO: remove print.
				}
				System.out.println("Adding cost of verification of " + nextSubRegex.toString());
				System.out.println(setCombinedStats.get(pastSubRegexes).getSelectivity() + " * " + amortizedCostOfVerification + " * " + totalNumberOfVerifications + " = " +
							(setCombinedStats.get(pastSubRegexes).getSelectivity() * amortizedCostOfVerification * totalNumberOfVerifications));
				expectedCost = expectedCost + 
						(
							setCombinedStats.get(pastSubRegexes).getSelectivity() * 
							amortizedCostOfVerification * 
							totalNumberOfVerifications
						);						
			}
			//
			pastSubRegexes.add(nextSubRegex);
		}
		return expectedCost;
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

    public static List<Span> computeMatchingResultsWithCompletePattern(Tuple inputTuple, RegexPredicate predicate, Pattern pattern, boolean reverse, RegexPredicate revPredicate, Pattern revPattern) {
        List<Span> matchingResults = new ArrayList<>();

        for (String attributeName : predicate.getAttributeNames()) {
            AttributeType attributeType = inputTuple.getSchema().getAttribute(attributeName).getAttributeType();
            String fieldValue = inputTuple.getField(attributeName).getValue().toString();

            // types other than TEXT and STRING: throw Exception for now
            if (attributeType != AttributeType.STRING && attributeType != AttributeType.TEXT) {
                throw new DataFlowException("KeywordMatcher: Fields other than STRING and TEXT are not supported yet");
            }

//            System.out.println(fieldValue);
//            System.out.println("--------------------------");
//            System.out.println(predicate.getRegex());
            if(reverse){
            	String reverseFieldValue = new StringBuilder(fieldValue).reverse().toString();
            	Matcher javaMatcher = revPattern.matcher(reverseFieldValue);
            	while (javaMatcher.find()) {
            		int start = fieldValue.length() - javaMatcher.end();
            		int end = fieldValue.length() - javaMatcher.start();
            		matchingResults.add(
            				new Span(attributeName, start, end, predicate.getRegex(), fieldValue.substring(start, end)));
            	}            	
            }else{
            	Matcher javaMatcher = pattern.matcher(fieldValue);
            	while (javaMatcher.find()) {
            		int start = javaMatcher.start();
            		int end = javaMatcher.end();
            		matchingResults.add(
            				new Span(attributeName, start, end, predicate.getRegex(), fieldValue.substring(start, end)));
            	}
            }
        }

        return matchingResults;
    }
    
    /*
     * Also returns overlapping and nested mathces. More expensive than computeMatchingResultsWithCompletePattern 
     * because it calls proceed() from the RegexMatcher rather than find().
     */
    public static List<Span> computeAllMatchingResults(Tuple inputTuple, SubRegex subRegex, boolean forcedExecutionDirection, boolean forcedReverse){
        List<Span> matchingResults = new ArrayList<>();

        for (String attributeName : subRegex.regex.getAttributeNames()) {
            AttributeType attributeType = inputTuple.getSchema().getAttribute(attributeName).getAttributeType();
            String fieldValue = inputTuple.getField(attributeName).getValue().toString();

            // types other than TEXT and STRING: throw Exception for now
            if (attributeType != AttributeType.STRING && attributeType != AttributeType.TEXT) {
                throw new DataFlowException("KeywordMatcher: Fields other than STRING and TEXT are not supported yet");
            }

			boolean runReverse = (forcedExecutionDirection && forcedReverse) || ((!forcedExecutionDirection) && subRegex.isReverseExecutionFaster());
			if(runReverse){
				String reverseFieldValue = new StringBuilder(fieldValue).reverse().toString();
				Matcher javaMatcher = subRegex.getReverseSubRegex().regexPatern.matcher(reverseFieldValue);
				while (javaMatcher.proceed()) {
					int start = reverseFieldValue.length() - javaMatcher.end();
					int end = reverseFieldValue.length() - javaMatcher.start();
					matchingResults.add(
							new Span(attributeName, start, end, subRegex.regex.getRegex(), fieldValue.substring(start, end)));
				}
			}else{
				Matcher javaMatcher = subRegex.regexPatern.matcher(fieldValue);
				while (javaMatcher.proceed()) {
					int start = javaMatcher.start();
					int end = javaMatcher.end();
					matchingResults.add(
							new Span(attributeName, start, end, subRegex.regex.getRegex(), fieldValue.substring(start, end)));
				}
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

    public static List<Span> computeAllMatchingResults(String attributeName, String src, SubRegex subRegex, boolean findAll){
    	Matcher matcher = subRegex.regexPatern.matcher(src);
    	List<Span> matchingResults = new ArrayList<>();
    	boolean hasMore = findAll? matcher.proceed(): matcher.find();
    	while(hasMore){
    		matchingResults.add(new Span(attributeName, 
    				matcher.start(), 
    				matcher.end(), 
    				subRegex.regex.getRegex(), 
    				src.substring(matcher.start(), matcher.end())));
    		hasMore = findAll? matcher.proceed(): matcher.find();
    	}
    	return matchingResults;
    }
    
    public static boolean doesMatchStartToEnd(String attributeName, String src, int start, int end, SubRegex subRegex){
    	if(start < 0 || start >= src.length() || end < 0 || end >= src.length()){
    		return false;
    	}
    	
//    	System.out.println(src.substring(start, end));
//    	System.out.println(subRegex.toString());
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
    

    public static double estimateNumberOfNeededVerifications(SubRegex subRegex, SubRegex leftBound, SubRegex rightBound){
    	if(leftBound == null && rightBound == null){ // No computed subRegex is around the subRegex
    		return subRegex.stats.matchingSrcAvgLen;
    	}else if(leftBound == null){ // && rightBound != null // one computed subRegex on right
    		if(subRegex.getEnd() == rightBound.getStart()){ // Direct right neighbor is computed.
    			return rightBound.stats.getTotalTfAverages();
    		}else{ // right bound isn't direct.
        		return subRegex.stats.matchingSrcAvgLen;
    		}
    	}else if(rightBound == null){ // && leftBound != null // one computed subRegex on left
    		if(subRegex.getStart() == leftBound.getEnd()){ // Direct left neighbor is computed.
    			return leftBound.stats.getTotalTfAverages();
    		}else{ // left bound isn't direct.
    			return subRegex.stats.matchingSrcAvgLen;
    		}
    	}else{ // rightBound != null && leftBound != null// two computed subRegexes on both sides
			if(leftBound.getEnd() == subRegex.getStart() && subRegex.getEnd() == rightBound.getStart()){ // left and right are both direct neighbors
				List<Double> leftTfAverages = leftBound.stats.getTfAveragesPerAttributes();
				List<Double> rightTfAverages = rightBound.stats.getTfAveragesPerAttributes();
				double totalProduct = 0;
				if(leftTfAverages.size() != rightTfAverages.size()){
					System.out.println("Something is wrong. number of tf stats not equal on left and right."); // TODO remove print.
				}
				for(int i = 0 ; i < leftTfAverages.size(); ++i){
					totalProduct += (leftTfAverages.get(i) * rightTfAverages.get(i));
				}
				return totalProduct;
				
			}else if(leftBound.getEnd() == subRegex.getStart()){ // only left is direct neighbor
				return leftBound.stats.getTotalTfAverages();
			}else if(subRegex.getEnd() == rightBound.getStart()){ // only right is direct neighbor
    			return rightBound.stats.getTotalTfAverages();
			}else { // no left nor right are direct neighbors
        		return subRegex.stats.matchingSrcAvgLen;
			}
    	}
    }
    public static int computeNumberOfNeededVerifications(Tuple inputTuple, SubRegex subRegex, 
    		SubRegex leftBound, SubRegex rightBound){
    	if(leftBound == null && rightBound == null){ // No computed subRegex is around the subRegex
        	// Default number of running the regex when there is no helping bound around it.
        	int totalSourceSize = 0;
        	for (String attributeName : subRegex.regex.getAttributeNames()) {
                AttributeType attributeType = inputTuple.getSchema().getAttribute(attributeName).getAttributeType();
                String fieldValue = inputTuple.getField(attributeName).getValue().toString();
                // types other than TEXT and STRING: throw Exception for now
                if (attributeType != AttributeType.STRING && attributeType != AttributeType.TEXT) {
                    throw new DataFlowException("KeywordMatcher: Fields other than STRING and TEXT are not supported yet");
                }
                totalSourceSize += fieldValue.length();
        	}
    		return totalSourceSize;
    	}else if(leftBound == null){ // && rightBound != null // one computed subRegex on right
    		List<Span> rightBoundSpans = rightBound.getLatestMatchingSpanList();
    		if(subRegex.getEnd() == rightBound.getStart()){ // Direct right neighbor is computed.
    			return rightBoundSpans.size();
    		}else{ // right bound isn't direct.
    			// start reverse matching (from right to left) from rightBound maximum of span starts to the left
    			int totalMatchingSize = 0;
    	        for (String attributeName : subRegex.regex.getAttributeNames()) {
    	            AttributeType attributeType = inputTuple.getSchema().getAttribute(attributeName).getAttributeType();
    	            String fieldValue = inputTuple.getField(attributeName).getValue().toString();
    	            // types other than TEXT and STRING: throw Exception for now
    	            if (attributeType != AttributeType.STRING && attributeType != AttributeType.TEXT) {
    	                throw new DataFlowException("KeywordMatcher: Fields other than STRING and TEXT are not supported yet");
    	            }
    	            // span list must be pruned for only those spans that belong to the same attribute.
    	            SpanListSummary rightBoundSummary = 
    	            		SpanListSummary.summerize(
    	            				rightBoundSpans.stream().filter(
    	            						s -> s.getAttributeName().equals(attributeName)).collect(Collectors.toList()));
    	            totalMatchingSize += (fieldValue.length() - rightBoundSummary.startMax);
    	        }
    	        return totalMatchingSize;
    		}
    	}else if(rightBound == null){ // && leftBound != null // one computed subRegex on left
    		List<Span> leftBoundSpans = leftBound.getLatestMatchingSpanList();
    		if(subRegex.getStart() == leftBound.getEnd()){ // Direct left neighbor is computed.
    			return leftBoundSpans.size();
    		}else{ // left bound isn't direct.
    			// start matching from leftBound minimum of span ends to the right
    			int totalMatchingSize = 0;
    	        for (String attributeName : subRegex.regex.getAttributeNames()) {
    	            AttributeType attributeType = inputTuple.getSchema().getAttribute(attributeName).getAttributeType();
    	            String fieldValue = inputTuple.getField(attributeName).getValue().toString();
    	            // types other than TEXT and STRING: throw Exception for now
    	            if (attributeType != AttributeType.STRING && attributeType != AttributeType.TEXT) {
    	                throw new DataFlowException("KeywordMatcher: Fields other than STRING and TEXT are not supported yet");
    	            }
    	            SpanListSummary leftBoundSummary = SpanListSummary.summerize(
    	            		leftBoundSpans.stream().filter(
            						s -> s.getAttributeName().equals(attributeName)).collect(Collectors.toList()));
    	            totalMatchingSize += leftBoundSummary.endMin;
    	        }
    	        return totalMatchingSize;
    		}
    	}else{ // rightBound != null && leftBound != null// two computed subRegexes on both sides
    		// start matching from the minimum end of the left bound spans to maximum start of the right bound spans
    		List<Span> leftBoundSpans = leftBound.getLatestMatchingSpanList();
    		List<Span> rightBoundSpans = rightBound.getLatestMatchingSpanList();
    		int totalNumOfVerifications = 0;
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
    						totalNumOfVerifications ++;
    					}
    				}
    				
    			}else if(leftBound.getEnd() == subRegex.getStart()){ // only left is direct neighbor
    				for(Span leftSpan: leftSpans){
    					int start = leftSpan.getEnd();
    					if(start >= rightBoundSummary.startMax){
    						continue;
    					}
    					totalNumOfVerifications ++;   					
    				}
    			}else if(subRegex.getEnd() == rightBound.getStart()){ // only right is direct neighbor
    				for(Span rightSpan: rightSpans){
    					int end = rightSpan.getStart();
    					if(end <= leftBoundSummary.endMin){
    						continue;
    					}
    					totalNumOfVerifications ++;
    				}
    				
    			}else { // no left nor right are direct neighbors
    				String fieldValueSubStr = fieldValue.substring(leftBoundSummary.endMin, rightBoundSummary.startMax);
    				totalNumOfVerifications += fieldValueSubStr.length();
    			}
    		}
	        return totalNumOfVerifications;
    	}
    }
    
    public static List<Span> computeSubRegexMatchesWithComputedNeighbors(Tuple inputTuple, 
    		SubRegex subRegex, 
    		SubRegex leftBound, SubRegex rightBound, 
    		int numberOfCoreSubRegexes, boolean forceExecutionDirection, boolean forceReverse){
    	
    	List<Span> matchingResults = new ArrayList<>();
    	
    	if(leftBound == null && rightBound == null){ // No computed subRegex is around the subRegex
    		if(subRegex.getStart() == 0 && subRegex.getEnd() == numberOfCoreSubRegexes){
    			return computeMatchingResultsWithCompletePattern(inputTuple, subRegex.regex, subRegex.regexPatern, false, null, null);
    		}else {
    			return computeAllMatchingResults(inputTuple, subRegex, false, false);
    		}
    	}else if(leftBound == null){ // && rightBound != null // one computed subRegex on right
    		List<Span> rightBoundSpans = rightBound.getLatestMatchingSpanList();
	        for (String attributeName : subRegex.regex.getAttributeNames()) {
	            AttributeType attributeType = inputTuple.getSchema().getAttribute(attributeName).getAttributeType();
	            String fieldValue = inputTuple.getField(attributeName).getValue().toString();
	            // types other than TEXT and STRING: throw Exception for now
	            if (attributeType != AttributeType.STRING && attributeType != AttributeType.TEXT) {
	                throw new DataFlowException("KeywordMatcher: Fields other than STRING and TEXT are not supported yet");
	            }
	            List<Span> rightSpans = rightBoundSpans.
	            		stream().filter(s -> s.getAttributeName().equals(attributeName)).
	            		collect(Collectors.toList());
	            if(subRegex.getEnd() == rightBound.getStart()){ // Direct right neighbor is computed.
    	            String reverseFieldValue = new StringBuilder(fieldValue).reverse().toString();
    	            for(Span rightSpan: rightSpans){
    	            	List<Span> reverseMatches = 
    	            			computeMatchingResultsStartingAt(attributeName, reverseFieldValue, 
    	            			fieldValue.length() - rightSpan.getStart(), subRegex.getReverseSubRegex(), subRegex.getStart() != 0);
    	            	for(Span s: reverseMatches){
    	            		matchingResults.add(new Span(attributeName, 
    	            				fieldValue.length() - s.getEnd(), 
    	            				fieldValue.length() - s.getStart(), 
    	            				subRegex.regex.getRegex(), 
    	            				fieldValue.substring(fieldValue.length() - s.getEnd(), fieldValue.length() - s.getStart())));
    	            	}
    	            }
	            }else{
	            	// start reverse matching (from right to left) from rightBound maximum of span starts to the left
	    			SpanListSummary rightBoundSummary = SpanListSummary.summerize(rightSpans);
    	            String reverseFieldValue = new StringBuilder(fieldValue).reverse().toString();
	            	List<Span> reverseMatches = 
	            			computeAllMatchingResults(attributeName, 
	            					reverseFieldValue.substring(fieldValue.length() - rightBoundSummary.startMax), 
	            			subRegex.getReverseSubRegex(), subRegex.getStart() != 0);
	            	for(Span s: reverseMatches){
	            		matchingResults.add(new Span(attributeName, 
	            				rightBoundSummary.startMax - s.getEnd(), 
	            				rightBoundSummary.startMax - s.getStart(), 
	            				subRegex.regex.getRegex(), 
	            				fieldValue.substring(rightBoundSummary.startMax - s.getEnd(), 
	            						rightBoundSummary.startMax - s.getStart())));
	            	}
	            }
	        }
    			
    	}else if(rightBound == null){ // && leftBound != null // one computed subRegex on left
    		List<Span> leftBoundSpans = leftBound.getLatestMatchingSpanList();
	        for (String attributeName : subRegex.regex.getAttributeNames()) {
	            AttributeType attributeType = inputTuple.getSchema().getAttribute(attributeName).getAttributeType();
	            String fieldValue = inputTuple.getField(attributeName).getValue().toString();
	            // types other than TEXT and STRING: throw Exception for now
	            if (attributeType != AttributeType.STRING && attributeType != AttributeType.TEXT) {
	                throw new DataFlowException("KeywordMatcher: Fields other than STRING and TEXT are not supported yet");
	            }
	            List<Span> leftSpans = leftBoundSpans.
	            		stream().filter(s -> s.getAttributeName().equals(attributeName)).
	            		collect(Collectors.toList());
	    		if(subRegex.getStart() == leftBound.getEnd()){ // Direct left neighbor is computed.
    	            for(Span leftSpan: leftSpans){
    	            	List<Span> spans = computeMatchingResultsStartingAt(attributeName, fieldValue, 
    	            			leftSpan.getEnd(), subRegex, subRegex.getEnd() != numberOfCoreSubRegexes);
    	            	matchingResults.addAll(spans);
    	            }
	    		}else{ // left bound isn't direct.
	    			// start matching from leftBound minimum of span ends to the right
	    			SpanListSummary leftBoundSummary = SpanListSummary.summerize(leftBoundSpans);
    	            List<Span> spans = computeAllMatchingResults(attributeName, 
    	            		fieldValue.substring(leftBoundSummary.endMin), subRegex, 
    	            		subRegex.getEnd() != numberOfCoreSubRegexes);
    	            for(Span s: spans){
    	            	matchingResults.add(new Span(s.getAttributeName(), s.getStart() + leftBoundSummary.endMin, 
    	            			s.getEnd() + leftBoundSummary.endMin, s.getKey(), s.getValue()));
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
    				String reverseFieldValue = null;
    				if(subRegex.isReverseExecutionFaster()){
    					reverseFieldValue = new StringBuffer(fieldValue).reverse().toString();
    				}
    				for(Span leftSpan: leftSpans){
    					int start = leftSpan.getEnd();
    					for(Span rightSpan: rightSpans){
    						int end = rightSpan.getStart();
    						if(end < start){
    							continue;
    						}
    						boolean runReverse = (forceExecutionDirection && forceReverse) || ((!forceExecutionDirection) && subRegex.isReverseExecutionFaster());
							if(runReverse){
    							// verify in reverse direction..
    							if(doesMatchStartToEnd(attributeName, reverseFieldValue, 
    									reverseFieldValue.length() - end, reverseFieldValue.length() - start, 
    									subRegex.getReverseSubRegex())){
    								matchingResults.add(new Span(attributeName, start, end, subRegex.regex.getRegex(), 
    										fieldValue.substring(start, end)));
    							}
							}else{
    							// verify in normal left to right direction
    							if(doesMatchStartToEnd(attributeName, fieldValue, start, end, subRegex)){
    								matchingResults.add(new Span(attributeName, start, end, subRegex.regex.getRegex(), 
    										fieldValue.substring(start, end)));
    							}
							}
    					}
    				}
    			}else if(leftBound.getEnd() == subRegex.getStart()){ // only left is direct neighbor
    				for(Span leftSpan: leftSpans){
    					int start = leftSpan.getEnd();
    					if(start >= rightBoundSummary.startMax){
    						continue;
    					}
    					List<Span> spans = computeMatchingResultsStartingAt(attributeName, 
    							fieldValue.substring(0, rightBoundSummary.startMax), 
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
    				String fieldValueSubStr = fieldValue.substring(leftBoundSummary.endMin, rightBoundSummary.startMax);
					boolean runReverse = (forceExecutionDirection && forceReverse) || ((!forceExecutionDirection) && subRegex.isReverseExecutionFaster());
					if(runReverse){
						// run in reverse order
    					String reverseFieldSubStr = new StringBuffer(fieldValueSubStr).reverse().toString();
    					List<Span> spans = computeAllMatchingResults(attributeName, reverseFieldSubStr, 
    							subRegex.getReverseSubRegex(), true);
    					for(Span s: spans){
    						matchingResults.add(new Span(s.getAttributeName(), 
    								rightBoundSummary.startMax - s.getEnd(), rightBoundSummary.startMax - s.getStart(), 
    								subRegex.regex.toString(),
    								fieldValue.substring(rightBoundSummary.startMax - s.getEnd(), 
    										rightBoundSummary.startMax - s.getStart())));
    					}
					}else{
						List<Span> spans = computeAllMatchingResults(attributeName, fieldValueSubStr, subRegex, true);
						for(Span s: spans){
							matchingResults.add(new Span(s.getAttributeName(), 
									s.getStart() + leftBoundSummary.endMin , s.getEnd() + leftBoundSummary.endMin , 
									s.getKey(), s.getValue()));
						}
					}
    			}
    		}
    	}
    	
    	return matchingResults;
    }
    
}