package edu.uci.ics.textdb.exp.regexmatcher;

import jregex.*;

import java.util.*;
import java.util.stream.Collectors;

import com.google.re2j.PublicParser;
import com.google.re2j.PublicRE2;
import com.google.re2j.PublicRegexp;
import com.google.re2j.PublicSimplify;
import com.google.re2j.PublicRegexp.PublicOp;
import com.mysql.fabric.xmlrpc.base.Array;

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
		if(failureCost != 0){ // Cost average won't be updated if the passed input is zero
			failureCostAverage = ((failureDataPointCounter * failureCostAverage) + failureCost ) / (failureDataPointCounter + 1);
		}
		failureDataPointCounter ++;
		addStatsMatchingSrcSize(matchingSrcSize);
	}
	
	public void addStatsSubRegexSuccess(double successCost, int matchingSrcSize){
		// update cost
		if(successCost != 0){ // Update cost average only if the passed input is greater than zero
			successCostAverage = ((successDataPointCounter * successCostAverage) + successCost ) / (successDataPointCounter + 1);
		}
		df ++;
		// update selectivity
		successDataPointCounter ++;
		// update matching src size
		addStatsMatchingSrcSize(matchingSrcSize);
	}
	
	public void addTfAveragePerAttribute(List<Integer> numberOfMatchSpans){
		if(successDataPointCounter == 0){
			System.out.println("Unexpected state. Adding tfAverage while success is not incremented."); // TODO: remove or replace with exception
			return;
		}
		successDataPointCounter --;
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
		successDataPointCounter ++;
	}
	
	private void addStatsMatchingSrcSize(int matchingSrcSize){
		matchingSrcAvgLen = (matchingSrcAvgLen * Math.max(getSize()-1,0)) + matchingSrcSize;
		matchingSrcAvgLen = matchingSrcAvgLen / Math.max(getSize(), 1);
	}
	
	public double getSelectivity(){
		double selectivity = (successDataPointCounter * 1.0 / getSize());
		if(selectivity == 0){
			return 0.001;
		}
		return selectivity;
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
	
	public double getConfidenceValue(){
		return 2.58 * Math.sqrt(getSelectivity() * (1 - getSelectivity()) / getSize() );
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
	int originalRegexSubsCount;
	
	RegexStats stats = null;
	
	// spans belong to the matches in the latest tuple
	List<Span> latestMatchingSpans = null;
	
	public SubRegex(){
		regexPatern = null;
		startWithRegexPattern = null;
		startToEndRegexPattern = null;
		startingCoreSubRegIndex = numberOfCoreSubRegexes = originalRegexSubsCount = -1;
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
		this.originalRegexSubsCount = 0;
	}
	
	public void setOriginalSubRegexCount(int count){
		originalRegexSubsCount = count;
		if(reverseSubRegex != null){
			reverseSubRegex.setOriginalSubRegexCount(count);
		}
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
	
	public String toStringShort(){
		return "[" + getStart() + "," + getEnd() + "] ";
	}
	
	public static String getPlanSignature(List<SubRegex> plan){
		String signature = "";
		for(SubRegex s: plan){
			signature += s.toStringShort();
		}
		return signature;
	}
	
	public boolean isReverseExecutionFaster(){
		// No reverse available or there is no stats collected for reverse 
		if(getReverseSubRegex() == null || getReverseSubRegex().stats.getSize() == 0){
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
	
	public boolean isFirstSubRegex(){
		return getStart() == 0;
	}
	
	public boolean isLastSubRegex(){
		return getEnd() == originalRegexSubsCount;
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
    // The candidate plans
    List<QueryPlan> queryPlans = new ArrayList<>();
    // Parallel map to isSetNonHighCovering. Records the combined selectivity of each set of sub-regexes.
    Map<Set<SubRegex>, RegexStats> setCombinedStats = new HashMap<>();
    // A map for memoization of the cost of candidate plans.
    Map<Set<SubRegex>, SubRegexCover> candidatePlanCosts = new HashMap<>();
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
//            populateCombinedSetStatsMap();

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
        boolean executed = false;
//        if (this.regexType == RegexType.NO_LABELS) {
    	if (this.regexType == this.regexType) {
    		for(QueryPlan plan : queryPlans){
    			if(plan.needsMoreStatistics()){
    				matchingResults = plan.process(inputTuple);
    				executed = true;
    			}
    		}
    		if(! executed){
    			double minCost = Double.MAX_VALUE;
    			QueryPlan bestPlan = null;
        		for(QueryPlan plan : queryPlans){
        			if(plan.getExpectedCost() < minCost){
        				bestPlan = plan;
        				minCost = plan.getExpectedCost();
        			}
        		}
        		matchingResults = bestPlan.process(inputTuple);
    		}
        } else if (this.regexType == RegexType.LABELED_WITH_QUALIFIERS) {
            matchingResults = labeledRegexProcessor.computeMatchingResults(inputTuple);
        } else {
            matchingResults = labledRegexNoQualifierProcessor.computeMatchingResults(inputTuple);
        }

        // matchingResults is ready. Will save it in the tuple.
        if (matchingResults == null || matchingResults.isEmpty()) {
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
						RegexMatcherUtils.computeMatchingResultsWithCompletePattern(inputTuple, 
								sub.regex, sub.regexPatern, true, 
								sub.getReverseSubRegex().regex, sub.getReverseSubRegex().regexPatern);
			}else{
				subRegexSpans = 
						RegexMatcherUtils.computeMatchingResultsWithCompletePattern(inputTuple, 
								sub.regex, sub.regexPatern, false, null, null);
			}
			// Save the span list for this subregex to be used in the second phase of the algorithm.
			sub.resetMatchingSpanList(subRegexSpans);
			return;
		}
		
		if(sub.complexity == SubRegex.ComplexityLevel.High){ // High sub-regex
			Set<SubRegex> pastSubRegexes = new HashSet<>();
			for(int i = 0 ; i < subRegexIndex; i++){
				pastSubRegexes.add(selectedSubRegexes.get(i));
			}
			SubRegex leftBound = RegexMatcherUtils.findSubRegexNearestComputedLeftNeighbor(pastSubRegexes, sub);
			SubRegex rightBound = RegexMatcherUtils.findSubRegexNearestComputedRightNeighbor(pastSubRegexes, sub);
			subRegexSpans = 
					RegexMatcherUtils.computeSubRegexMatchesWithComputedNeighbors(inputTuple, sub, 
							leftBound, rightBound, false, false); // no force on execution direction
			// Save the span list for this subregex to be used in the second phase of the algorithm.
			sub.resetMatchingSpanList(subRegexSpans);
			return;
		}else { // Non-high sub-regex
			subRegexSpans = RegexMatcherUtils.computeAllMatchingResults(inputTuple, sub, false, false); // no force on execution direction
			// also save the span list for this subregex to be used in the second phase of the algorithm.
			sub.resetMatchingSpanList(subRegexSpans);
			return;
		}
    }
    
	public void runAndCollectStatistics(Tuple inputTuple, SubRegex sub){
		// first clean the data structures from the previous tuples
		sub.resetMatchingSpanList(null);
		// If it's the original complete regex, we can collect more precise statistics
		if(sub.getStart() == 0 && sub.getEnd() == coreSubRegexes.size()){
			// left to right execution (normal)
			{
				double oldExpectedCost = sub.stats.getExpectedCost();
				
				long startMatchingTime = System.nanoTime();
				List<Span> subRegexSpans = 
						RegexMatcherUtils.computeMatchingResultsWithCompletePattern(inputTuple, 
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
    				sub.stats.addStatsSubRegexSuccess(matchingCost, totalTupleTextSize);
    				sub.stats.addTfAveragePerAttribute(matchSizesPerAttributes);
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
						RegexMatcherUtils.computeMatchingResultsWithCompletePattern(inputTuple, 
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
    				sub.getReverseSubRegex().stats.addStatsSubRegexSuccess(matchingCost, totalTupleTextSize);
    				sub.getReverseSubRegex().stats.addTfAveragePerAttribute(matchSizesPerAttributes);
				}
			}
			return;
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
						RegexMatcherUtils.computeSubRegexMatchesWithComputedNeighbors(inputTuple, sub, 
								leftBound, rightBound, true, false); // force not to use reverse execution
				long endMatchingTime = System.nanoTime();
				long matchingTime = endMatchingTime - startMatchingTime;
				int numVerifications = RegexMatcherUtils.computeNumberOfNeededVerifications(inputTuple, sub, leftBound, rightBound);
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
					sub.stats.addStatsSubRegexSuccess(verificationCost, numVerifications);
					sub.stats.addTfAveragePerAttribute(matchSizesPerAttributes);
				}
			}
			{	// measure the time of verification if we use reverse order
				long startMatchingTime = System.nanoTime();
				List<Span> subRegexSpans = 
						RegexMatcherUtils.computeSubRegexMatchesWithComputedNeighbors(inputTuple, sub, 
								leftBound, rightBound, true, true); // force to use reverse execution
				long endMatchingTime = System.nanoTime();
				long matchingTime = endMatchingTime - startMatchingTime;
				int numVerifications = RegexMatcherUtils.computeNumberOfNeededVerifications(inputTuple, sub, leftBound, rightBound);
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
					sub.getReverseSubRegex().stats.addStatsSubRegexSuccess(verificationCost, numVerifications);
					sub.getReverseSubRegex().stats.addTfAveragePerAttribute(matchSizesPerAttributes);
				}
			}
		}else{
			// SubRegex doesn't have * or +, so we will find all matches to also measure tf_average
			// Collect statistics for running normally
			{
				long startMatchingTime = System.nanoTime();
				List<Span> subRegexSpans = RegexMatcherUtils.computeAllMatchingResults(inputTuple, sub, true, false); // run normally
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
					sub.stats.addStatsSubRegexSuccess(matchingCost, totalTupleTextSize);
					sub.stats.addTfAveragePerAttribute(matchSizesPerAttributes);
				}
			}
			// Collect statistics for running in reverse
			{
				long startMatchingTime = System.nanoTime();
				List<Span> subRegexSpans = RegexMatcherUtils.computeAllMatchingResults(inputTuple, sub, true, true); // run in reverse
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
					sub.getReverseSubRegex().stats.addStatsSubRegexSuccess(matchingCost, totalTupleTextSize);
					sub.getReverseSubRegex().stats.addTfAveragePerAttribute(matchSizesPerAttributes);
				}
			}
		}
		
		return;
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
//        System.out.println("First phase time : " + firstPhaseTime * 1.0 / 1000000000);
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
        for(SubRegex sub : coreSubRegexes){
        	sub.setOriginalSubRegexCount(numberOfCoreSubRegexes);
        }
        mainRegex.setOriginalSubRegexCount(numberOfCoreSubRegexes);
        if(numberOfCoreSubRegexes != coreSubRegexes.size()){
        	System.out.println("Something wrong. The content of coreSubRegexes is not as expected."); // TODO maybe never happens 
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
    
    
    // Finds the sub-regex starting with start and goin to end from the subRegexContainer
    private SubRegex getSubRegex(int start, int end){
    	for(int i = 0 ; i < subRegexContainer.get(start).size(); i++){
    		if(subRegexContainer.get(start).get(i).getEnd() == end){
    			return subRegexContainer.get(start).get(i);
    		}
    	}
    	return null;
    }

    // Initializes the subRegexContainer structure with all possible expansions of 
    // non-high sub-regexes
    private void generateExpandedSubRegexes(){
    	
    	// initialize the subRegex container
    	for(int startingCSRIndex = 0 ; startingCSRIndex < coreSubRegexes.size(); ++startingCSRIndex){
    		subRegexContainer.add(new ArrayList<>());
    	}
    	// If it's high:
    	// 				make the longest possible sub-regex starting from here ending before the next non-high sub-regex
    	// If it's non-high:
    	// 				just put the core sub-regex on that position and go to the next iteration
    	// Note: Non-high and high sub-regexes are not mixed.
    	for(int startingCSRIndex = 0 ; startingCSRIndex < coreSubRegexes.size(); ++startingCSRIndex){
    		
    		SubRegex coreSubRegex = coreSubRegexes.get(startingCSRIndex);
    		RegexPredicate coreRegexPredicate = coreSubRegex.regex;
    		
    		if(coreSubRegex.complexity != SubRegex.ComplexityLevel.High){
        		if(coreSubRegex.getStart() == 0 && coreSubRegex.getEnd() == coreSubRegexes.size()){
        			// this is the full regex, we have previously prepared this regex in the break function
        			subRegexContainer.get(startingCSRIndex).add(mainRegex);
        		}else{
        			subRegexContainer.get(startingCSRIndex).add(coreSubRegex);
        		}
        		continue;
    		}
    		// High starting point
    		
    		RegexPredicate expandingPredicate = new RegexPredicate(coreRegexPredicate.getRegex(), 
    				coreRegexPredicate.getAttributeNames(),
    				coreRegexPredicate.isIgnoreCase(),
    				coreRegexPredicate.getSpanListName()+startingCSRIndex);
    		SubRegex.ComplexityLevel expandingComplexityLevel = coreSubRegex.complexity;
    		RegexPredicate reverseExpandingPredicate = new RegexPredicate(coreSubRegex.getReverseSubRegex().regex);

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
    	}
    		
    	// Also add a sub-regex starting from zero with full length which would be the 
    	// original complete regex.
    	
    	// Check the longest sub-regex starting from zero
    	SubRegex sub = subRegexContainer.get(0).get(subRegexContainer.get(0).size() - 1);
    	if(! (sub.getStart() == 0 && sub.getEnd() == coreSubRegexes.size())){ // the regex was actually broken.
    		// the large complete regex which starts from zero and goes to the end
    		subRegexContainer.get(0).add(mainRegex);
    	}
    	// Prepare query plans
    	queryPlans.add(new SingleRegexPlan(mainRegex));
    	if(coreSubRegexes.size() > 1){
    		List<SubRegex> subRegexCover = new ArrayList<>();
    		for(int start = 0 ; start < subRegexContainer.size(); start ++){
    			SubRegex next = subRegexContainer.get(start).get(0);
    			subRegexCover.add(next);
    			start = next.getEnd() - 1;
    		}
    		if(subRegexCover.size() > 1){
    			queryPlans.add(new MultiRegexPlan(subRegexCover));
    		}
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
    			pair.getValue().addStatsSubRegexSuccess(0, 0);
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
        
        // 1. First generate those set keys that contain only non-high sub-regexes.
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
    
    
    // This function recieves a set of core sub-regexes and tried to merge those 
    // elements that are adjacent together.
    // For example:
    // If it recieves {[0,1], [1,2], [4,5]}
    // It will output {[0, 2], [4,5]}
    private void mergeAdjacentElements(Set<SubRegex> elements, Set<SubRegex> maximizedElements){
    	while(true){
    		SubRegex toMergeLeft = null;
    		SubRegex toMergeRight = null;
    		boolean matched = false;
    		for(SubRegex outerSub : elements){
    			if(outerSub.complexity == SubRegex.ComplexityLevel.High){
    				continue;
    			}
        		for(SubRegex innerSub : elements){
        			if(innerSub.complexity == SubRegex.ComplexityLevel.High){
        				continue;
        			}
        			if(outerSub.getEnd() == innerSub.getStart()){
        				toMergeLeft = outerSub;
        				toMergeRight = innerSub;
        				matched = true;
        				break;
        			}else if(innerSub.getEnd() == outerSub.getStart() ){
        				toMergeLeft = innerSub;
        				toMergeRight = outerSub;
        				matched = true;
        				break;        				
        			}
        		}
        		if(matched){
        			break;
        		}
    		}
    		if(! matched){ // no adjacent elements left
    			maximizedElements.addAll(elements);
    			return;
    		}
    		SubRegex mergedSub = getSubRegex(toMergeLeft.getStart(), toMergeRight.getEnd());
    		elements.remove(toMergeLeft);
    		elements.remove(toMergeRight);
    		elements.add(mergedSub);
    		matched = false;
    	}
    }
    
    
    // This function breaks every compound element in the input and makes a new possibly larger set
    // For example:
    // Input: {[0,2], [4,6]}
    // Output: {[0,1], [1,2], [4,5], [5,6]}
    private void breakToCoreElements(Set<SubRegex> elements, Set<SubRegex> coreElements){
    	for(SubRegex s : elements){
    		if(s.complexity == SubRegex.ComplexityLevel.High){
    			coreElements.add(s);
    		}else{
    			if(s.getStart() + 1 < s.getEnd()){ // So it's a compound sub-regex
    				for(int i = s.getStart() ; i < s.getEnd(); i++){
    					coreElements.add(getSubRegex(i, i+1));
    				}
    			}
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
    	if(highSubRegexMode){
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
    			// Just skip over non-high sub-regexes because we are in highSubRegexMode
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
//    		System.out.println(subRegCover);
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
    			findOptimumPermutation(partialCoverClone, completeCoverReOrdered);
    			coverCosts.add(completeCoverReOrdered);
    			return;
    		}
    		// continue expanding the partial cover
    		collectPlanCostEstimationsStartingAt(startingCSR + subRegex.numberOfCoreSubRegexes, partialCoverClone, coverCosts);
    	}
    }
    
    private void findOptimumPermutation(Set<SubRegex> elements, Set<SubRegex> previouslyUseElements, SubRegexCover optimumPermutation){
    	// Out most call can get previouslyUseElements null which means no sub-regexes where used before
    	if(previouslyUseElements == null){
    		previouslyUseElements = new HashSet<>();
    	}
    	if(! optimumPermutation.subRegexes.isEmpty()){
    		System.out.println("Unexpected input. OUT parameter must be empty but it's not.");
    	}
    	
    	// The selectivity that should apply to these elements
    	double preSelectivity = estimateSetSelectivity(previouslyUseElements);
    	// 1. check for memoization
    	if(candidatePlanCosts.containsKey(elements)){
    		SubRegexCover c = candidatePlanCosts.get(elements);
    		optimumPermutation.subRegexes.addAll(c.subRegexes);
    		optimumPermutation.expectedCost = c.expectedCost ;
    		return;
    	}
    	
    	
    	// 2. check for the end case where elements has one element in it
    	if(elements.size() == 1){
    		for(SubRegex nextSubRegex : elements){ // We know it has only one element but since it's set we have to iterate
    			// First calculate the expected cost of s
    			double nextSubRegexCost = 0;
    			if(nextSubRegex.getStart() == 0 && nextSubRegex.getEnd() == coreSubRegexes.size()){ // The complete regex case
    				nextSubRegexCost = nextSubRegex.getExpectedCost() * nextSubRegex.stats.matchingSrcAvgLen;
    			}else{
    				if(nextSubRegex.complexity == SubRegex.ComplexityLevel.High){
    					// Find the closest computed sub-regex to the left
    					SubRegex leftSub = RegexMatcherUtils.findSubRegexNearestComputedLeftNeighbor(previouslyUseElements, nextSubRegex);
    					// Find the closest computed sub-regex to the right
    					SubRegex rightSub = RegexMatcherUtils.findSubRegexNearestComputedRightNeighbor(previouslyUseElements, nextSubRegex);
    					double totalNumberOfVerifications = estimateNumberOfNeededVerifications(nextSubRegex, leftSub, rightSub);
    					nextSubRegexCost = nextSubRegex.getExpectedCost() * totalNumberOfVerifications;
    				}else{
    					nextSubRegexCost = nextSubRegex.getExpectedCost() * nextSubRegex.stats.matchingSrcAvgLen;
    				}
    			}
    			
    			optimumPermutation.subRegexes.add(nextSubRegex);
    			optimumPermutation.expectedCost = preSelectivity * nextSubRegexCost;
    			// Save in memoization map
    			SubRegexCover forMemoization = new SubRegexCover();
    			forMemoization.subRegexes.add(nextSubRegex);
    			forMemoization.expectedCost = preSelectivity * nextSubRegexCost;
    			candidatePlanCosts.put(elements, forMemoization);
    		}
    		return;
    	}
    	
    	
    	
    	optimumPermutation.expectedCost = Double.MAX_VALUE;
    	Set<SubRegex> newElements = new HashSet<>();
    	newElements.addAll(elements);
    	// Try High sub-regexes only for the case where we have no non-high sub-regex left
    	for(SubRegex s : elements){
    		// Remember cost of s
    		double sCost = 0;
			if(s.getStart() == 0 && s.getEnd() == coreSubRegexes.size()){ // The complete regex case
				sCost = s.getExpectedCost() * s.stats.matchingSrcAvgLen;
			}else{
				if(s.complexity == SubRegex.ComplexityLevel.High){
					// Find the closest computed sub-regex to the left
					SubRegex leftSub = RegexMatcherUtils.findSubRegexNearestComputedLeftNeighbor(previouslyUseElements, s);
					// Find the closest computed sub-regex to the right
					SubRegex rightSub = RegexMatcherUtils.findSubRegexNearestComputedRightNeighbor(previouslyUseElements, s);
					double totalNumberOfVerifications = estimateNumberOfNeededVerifications(s, leftSub, rightSub);
					sCost = s.getExpectedCost() * totalNumberOfVerifications;
				}else{
					sCost = s.getExpectedCost() * s.stats.matchingSrcAvgLen;
				}
			}
			// Remove s and do recursion
    		newElements.remove(s);
    		previouslyUseElements.add(s);
    		SubRegexCover subsetOptimum = new SubRegexCover();
    		findOptimumPermutation(newElements, previouslyUseElements, subsetOptimum);
    		// calculate combined cost
    		double combinedCost = preSelectivity * sCost + subsetOptimum.expectedCost;
    		if(combinedCost < optimumPermutation.expectedCost){
    			optimumPermutation.subRegexes.clear();
    			optimumPermutation.subRegexes.add(s);
    			optimumPermutation.subRegexes.addAll(subsetOptimum.subRegexes);
    			optimumPermutation.expectedCost = combinedCost;
    		}
    		// Fix the newElements and previouslyUseElements variables for next iteration
    		newElements.add(s);
    		previouslyUseElements.remove(s);
    	}
    	// The optimumPermutation variable is ready for return
    	// Just save in memoization map before returning
		SubRegexCover forMemoization = new SubRegexCover();
		forMemoization.subRegexes.addAll(optimumPermutation.subRegexes);
		forMemoization.expectedCost = optimumPermutation.expectedCost;
    	candidatePlanCosts.put(elements, forMemoization);
    	return;
    }
    
    
    // finds the best order of matching of the sub regexes in the given cover. 
    // outputs the corresponding order in the input optimumReorderedCover input.
    private void findOptimumPermutation(List<SubRegex> cover, SubRegexCover optimumReorderedCover){

    	Set<SubRegex> elements = new HashSet<>();
    	Set<SubRegex> preSelectedElements = new HashSet<>();
    	for(int i =0 ; i < cover.size(); ++i){
    		if(cover.get(i).complexity != SubRegex.ComplexityLevel.High){
    			elements.add(cover.get(i));
    		}
    	}
    	SubRegexCover nonHighOptimumPermutation = new SubRegexCover();
    	preSelectedElements.clear();
    	findOptimumPermutation(elements, preSelectedElements, nonHighOptimumPermutation);
    	preSelectedElements.clear();
    	preSelectedElements.addAll(elements);
    	elements.clear();
    	for(int i =0 ; i < cover.size(); ++i){
    		if(cover.get(i).complexity == SubRegex.ComplexityLevel.High){
    			elements.add(cover.get(i));
    		}
    	}
    	SubRegexCover highOptimumPermutation = new SubRegexCover();
    	findOptimumPermutation(elements, preSelectedElements, highOptimumPermutation);
    	
    	optimumReorderedCover.subRegexes.addAll(nonHighOptimumPermutation.subRegexes);
    	optimumReorderedCover.subRegexes.addAll(highOptimumPermutation.subRegexes);
    	optimumReorderedCover.expectedCost = nonHighOptimumPermutation.expectedCost + highOptimumPermutation.expectedCost;
    }
    
    public double estimateSetSelectivity(Set<SubRegex> elements){
    	// Not selective at all if empty
    	if(elements.isEmpty()){
    		return 1.0;
    	}
    	
    	// Check if a separate selectivity is measured for the set itself
    	if(setCombinedStats.containsKey(elements)){
    		return setCombinedStats.get(elements).getSelectivity();
    	}else{
    		// See if we can at least combined selectivity for sub-sets
    		//TODO but for now just multiple ...
    		System.out.println("Unexpected state. Set of sub-regexes is not included in the stats set.");
    		return 0.0;
    	}
    	
    }
    
	// The input cover is a list of SubRegexes that will be computed in the same order to 
    // find the matching results of the original regex.
	public double estimateExpectedCostOfCandidatePlan(List<SubRegex> plan, Set<SubRegex> previouslyUsedPlanSubs){
		// The outmost call be made with passing null for previouslyUsedPlanSubs
		if(previouslyUsedPlanSubs == null){
			previouslyUsedPlanSubs = new HashSet<>();
		}
		double preUsedSelectivity = estimateSetSelectivity(previouslyUsedPlanSubs);
		if(plan == null || plan.size() == 0) {
			System.out.println("Unexpected state. Something wrong. Trying to cost empty candidate plan."); // TODO: remove print
			return Double.MAX_VALUE;
		}
		Set<SubRegex> planElements = new HashSet<>();
		planElements.addAll(plan);
		// Check if cost of this plan is memoized
		if(candidatePlanCosts.containsKey(planElements)){
			System.out.println("SubRegex.getPlanSignature(plan) Memoized : " + candidatePlanCosts.get(SubRegex.getPlanSignature(plan)));
			return candidatePlanCosts.get(planElements).expectedCost;
		}
		// End case
		if(plan.size() == 1){
			SubRegex sub = plan.get(0);
			System.out.println(SubRegex.getPlanSignature(plan) + " >> " + preUsedSelectivity +" * "+ sub.getExpectedCost() +" * "+ sub.stats.matchingSrcAvgLen + " = " + (preUsedSelectivity * sub.getExpectedCost() * sub.stats.matchingSrcAvgLen));
			return preUsedSelectivity * sub.getExpectedCost() * sub.stats.matchingSrcAvgLen;
		}
		////////////////////////
		// Not computed before. Compute recursively.
		SubRegex nextSubRegex = null;
		int nextSubRegexIndex = -1;
		Iterator<SubRegex> iter = null;
		// 1. Find the next sub-regex to run. First try to find non-high and if nothing left, find a high sub-regex.
		{
			int i = 0 ;
			for(iter = plan.listIterator(); iter.hasNext(); ){
				SubRegex sub = iter.next();
				if(sub.complexity != SubRegex.ComplexityLevel.High){
					nextSubRegex = sub;
					nextSubRegexIndex = i;
					break;
				}
				i++;
			}
			if(nextSubRegex == null){
				iter = null;
				i = 0;
				for(iter = plan.listIterator(); iter.hasNext(); ){
					SubRegex sub = iter.next();
					if(sub.complexity == SubRegex.ComplexityLevel.High){
						nextSubRegex = sub;
						nextSubRegexIndex = i;
						break;
					}
					i++;
				}		
			}
		}
		// 2. Get the expected cost of running the nextSubRegex
		double nextSubRegexCost = 0;
		{
			if(nextSubRegex.complexity == SubRegex.ComplexityLevel.High){
				// Find the closest computed sub-regex to the left
				planElements.remove(nextSubRegex);
				SubRegex leftSub = RegexMatcherUtils.findSubRegexNearestComputedLeftNeighbor(planElements, nextSubRegex);
				// Find the closest computed sub-regex to the right
				SubRegex rightSub = RegexMatcherUtils.findSubRegexNearestComputedRightNeighbor(planElements, nextSubRegex);
				double totalNumberOfVerifications = estimateNumberOfNeededVerifications(nextSubRegex, leftSub, rightSub);
				nextSubRegexCost = nextSubRegex.getExpectedCost() * totalNumberOfVerifications;
			}else{
				nextSubRegexCost = nextSubRegex.getExpectedCost() * nextSubRegex.stats.matchingSrcAvgLen;
			}
		}
		
		// 3. Remove the nextSubRegex from the plan and add it to previouslyUsedPlanSubs to make the recursive call
		List<SubRegex> planTmp = new ArrayList<>();
		planTmp.addAll(plan);
		iter.remove();
		previouslyUsedPlanSubs.add(nextSubRegex);
		// 4. Calculate the cost of the rest of the plan calculated recursively and do memoization
		double subPlanCost = estimateExpectedCostOfCandidatePlan(plan, previouslyUsedPlanSubs);
		SubRegexCover subPlanCover = new SubRegexCover();
		subPlanCover.expectedCost = subPlanCost;
		subPlanCover.subRegexes.addAll(plan);
		candidatePlanCosts.put(planElements, subPlanCover);
		// 5. Calculate the cost of this plan based on the recursion results
		double cost = preUsedSelectivity * 
				(	nextSubRegexCost
					+ 
					subPlanCost
				);
		// 6. Fix the input before returning.
		plan.clear();
		plan.addAll(planTmp);
		previouslyUsedPlanSubs.remove(nextSubRegex);
		System.out.println(SubRegex.getPlanSignature(plan) + " >> " + preUsedSelectivity +" * (" + nextSubRegexCost + " + " + subPlanCost + ") = " + cost);
		return cost;
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
    
}