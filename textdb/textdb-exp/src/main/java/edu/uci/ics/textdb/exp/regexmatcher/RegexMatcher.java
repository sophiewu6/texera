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

import edu.stanford.nlp.ling.Label;
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

abstract class SubSequence{
	// CSR = CoreSubRegex
	int startingSubSeqIndex;
	int numberOfSubSequences;
	int originalSubSeqsCount;
	// spans belong to the matches in the latest tuple
	List<Span> latestMatchingSpans = null;
	// Structure for keeping statistics
	RegexStats stats = null;
	
	public SubSequence(int start, int length){
		startingSubSeqIndex = start;
		numberOfSubSequences = length;
	}
	
	public int getOriginalSubSeqsCount(){
		return originalSubSeqsCount;
	}
	public void setOriginalSubCount(int count){
		originalSubSeqsCount = count;
	}
	
	public int getStart(){
		return startingSubSeqIndex;
	}
	public int getEnd(){
		return startingSubSeqIndex + numberOfSubSequences;
	}
	
	public boolean isFirstSubSequence(){
		return getStart() == 0;
	}
	
	public boolean isLastSubSequence(){
		return getEnd() == getOriginalSubSeqsCount();
	}
	
	public List<Span> getLatestMatchingSpanList(){
		return latestMatchingSpans;
	}
	
	public void resetMatchingSpanList(List<Span> spanList){
		latestMatchingSpans = spanList;
	}
	
	public String toStringShort(){
		return "[" + getStart() + "," + getEnd() + ") ";
	}
	
	public abstract boolean isSubRegex();
	
	public abstract List<String> getAttributeNames();
}

class SubRegex extends SubSequence{
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
	
	public SubRegex(){
		super(-1, -1);
		super.setOriginalSubCount(-1);
		regexPatern = null;
		startWithRegexPattern = null;
		startToEndRegexPattern = null;
		this.complexity = ComplexityLevel.Low;
		reverseSubRegex = null;
	}
	public SubRegex(RegexPredicate predicate, int startingCSRIndex, int numberOfCSRs, ComplexityLevel complexity){
		super(startingCSRIndex, numberOfCSRs);
		super.setOriginalSubCount(0);
		regexPatern = new Pattern(predicate.getRegex());
		startWithRegexPattern = new Pattern("^" + predicate.getRegex());
		startToEndRegexPattern = new Pattern("^" + predicate.getRegex() + "$");
		regex = predicate;
		this.complexity = complexity;
		reverseSubRegex = null;
		stats = new RegexStats(regex.getAttributeNames().size());
	}
	
	public void setOriginalSubCount(int count){
		super.setOriginalSubCount(count);
		if(reverseSubRegex != null){
			reverseSubRegex.setOriginalSubCount(count);
		}
	}
	
	public void setReverseSubRegex(SubRegex reverse){
		reverseSubRegex = reverse;
	}
	
	public SubRegex getReverseSubRegex(){
		return reverseSubRegex;
	}
	
	public String toString(){
		return toStringShort() + "///" + complexity + "///" + regex.getRegex();
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
	
	@Override
	public boolean isSubRegex() {
		return true;
	}
	@Override
	public List<String> getAttributeNames() {
		return regex.getAttributeNames();
	}
}

class LabelSubSequence extends SubSequence{
	String labelName = "";
	List<String> attributeNames = null;
	public LabelSubSequence(int start, int length, String labelName, List<String> attrNames){
		super(start, length);
		this.labelName = labelName;
		this.attributeNames = attrNames;
		stats = new RegexStats(attributeNames.size());
	}
	
	public String toString(){
		return toStringShort() + "<<" + labelName + ">>";
	}

	@Override
	public boolean isSubRegex() {
		return false;
	}

	@Override
	public List<String> getAttributeNames() {
		return attributeNames;
	}
}
/**
 * Created by chenli on 3/25/16.
 *
 * @author Shuying Lai (laisycs)
 * @author Zuozhi Wang (zuozhiw)
 */
public class RegexMatcher extends AbstractSingleInputOperator {

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
    public static final String CHECK_REGEX_LABEL = "<<[^<>\\\\]*>>";
    public static final String LABEL_REGEX_PLACE_HOLDER = "(########)";

    public static final int MAX_TUPLES_FOR_STAT_COLLECTION = 1000;


    private boolean hasLabels = false;
    // pattern not null and used only if regex is not breakable and should always run in one part.
    private RegexPredicate mainRegexPredicate;
    private SubRegex mainRegex;
    
    private Schema inputSchema;

    // List of granular sub-regexes.
    List<SubSequence> coreSubSequences = new ArrayList<>();
    // The main container of all sub-regexes.
    List<List<SubSequence>> subRegexContainer = new ArrayList<>();
    // The candidate plans
    List<QueryPlan> queryPlans = new ArrayList<>();

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

        // Check if labeled or unlabeled
        hasLabels = checkForLabels();
        if (! hasLabels) {
        	// Case of having no labels
//        	pattern = predicate.isIgnoreCase() ? Pattern.compile(predicate.getRegex(), Pattern.CASE_INSENSITIVE) : Pattern.compile(predicate.getRegex());

            // set up the needed data structures for optimization and dynamic 
            // evaluation
            // 1. break the regex into a number of sub-regexes that are called CoreSubRegexes or CSRs here.
            breakIntoCoreSubregexes(null);
            if(coreSubSequences.size() == 1){
            	System.out.println("The original regex is not breakable."); //TODO remove the print
            }
            // 2. Expanding the core sub regexes to make compound sub regexes.
            generateExpandedSubRegexes();
            
            // 3. This will continue with collecting/using stats in processOneInputTuple.
            //    Note. Even if the regex is not breakable, we still collect statistics to see 
            //          if it's faster to run the regex in reverse direction.
            
        
        } else{
        	// Case of having labels
        	String mainRegexStr = mainRegexPredicate.getRegex();
        	String newRegexStr = "";
        	int startCopy = 0;
        	Matcher labelMatcher = new Pattern(CHECK_REGEX_LABEL).matcher(mainRegexStr);
        	List<String> labels = new ArrayList<>();
        	while(labelMatcher.find()){
        		int start = labelMatcher.start();
        		int end = labelMatcher.end();
        		newRegexStr += mainRegexStr.substring(startCopy, start);
        		newRegexStr += LABEL_REGEX_PLACE_HOLDER;
        		startCopy = end;
        		labels.add(mainRegexStr.substring(start+2, end-2));
        	}
        	newRegexStr += mainRegexStr.substring(startCopy);
        	mainRegexPredicate.setRegex(newRegexStr);
        	// 1. break the regex into a number of sub-regexes that are called CoreSubRegexes or CSRs here.
            breakIntoCoreSubregexes(labels);
            // 2. Expanding the core sub regexes to make compound sub regexes.
            generateExpandedSubRegexes();
            // 3. This will continue with collecting/using stats in processOneInputTuple.
            //    Note. Even if the regex is not breakable, we still collect statistics to see 
            //          if it's faster to run the regex in reverse direction.
        }
    }

    /*
     * Determines whether or not the regex has labels in it.
     */
    private boolean checkForLabels() {
        Matcher labelMatcher = new Pattern(CHECK_REGEX_LABEL).matcher(mainRegexPredicate.getRegex());
        if (! labelMatcher.find()) {
            return false;
        }
        return true;
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

        if(hasLabels && coreSubSequences.size() == 1){
    		//just read the spans from the tuple and write it in the right spanListName
			LabelSubSequence labelSub = (LabelSubSequence)coreSubSequences.get(0);
	        ListField<Span> labelSpanListField = inputTuple.getField(labelSub.labelName);
			
	        ListField<Span> outputSpanListField = inputTuple.getField(mainRegexPredicate.getSpanListName());
	        List<Span> spanList = outputSpanListField.getValue();
	        spanList.addAll(labelSpanListField.getValue());
	        return inputTuple;
        }

        List<Span> matchingResults = null;
        boolean executed = false;
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

        // matchingResults is ready. Will save it in the tuple.
        if (matchingResults == null || matchingResults.isEmpty()) {
            return null;
        }

        ListField<Span> spanListField = inputTuple.getField(mainRegexPredicate.getSpanListName());
        List<Span> spanList = spanListField.getValue();
        spanList.addAll(matchingResults);

        return inputTuple;
    }
    
    @Override
    protected void cleanUp() throws DataFlowException {
        if (! hasLabels) {
//        	System.out.println("No label regex operator closed.");
        } else {
//        	System.out.println("Labeled regex operator closed.");
        }
//        System.out.println("First phase time : " + firstPhaseTime * 1.0 / 1000000000);
    }

    public RegexPredicate getPredicate() {
        return mainRegexPredicate;
    }
    
    // If possible, breaks the main regex and populates the coreSubRegexes vector
    // If mainRegexPredicate is not breakble, it puts it entirely as the only subregex in 
    // coreSubRegexes vector.
    private void breakIntoCoreSubregexes(List<String> labels){
    	
    	int labelsIndex = 0;
    	
    	PublicRegexp re = PublicParser.parse(mainRegexPredicate.getRegex(), PublicRE2.PERL);
        re = PublicSimplify.simplify(re);
        
        int numberOfCoreSubRegexes = 0;
        SubRegex.ComplexityLevel mainRegexComplexity = SubRegex.ComplexityLevel.Low;
        RegexPredicate mainReverseRegexPredicate = 
        		new RegexPredicate("", mainRegexPredicate.getAttributeNames(), 
        				mainRegexPredicate.getSpanListName());
        if(re.getOp() != PublicRegexp.PublicOp.CONCAT){
        	if(re.toString().equals(LABEL_REGEX_PLACE_HOLDER)){
        		coreSubSequences.add(new LabelSubSequence(0, 1, labels.get(0), mainRegexPredicate.getAttributeNames()));
        	}else{
        		mainRegexComplexity = getRegexComplexity(re);
        		mainReverseRegexPredicate.setRegex(PublicRegexp.reverseDeepCopy(re).toString());
        	}
        	numberOfCoreSubRegexes = 1;
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
        		if(sub.toString().equals(LABEL_REGEX_PLACE_HOLDER)){
        			if(labels == null || labels.isEmpty()){
        				System.out.println("Unexpected state: labels list is empty even though we found the place holder.");//TODO: remove or replace with exception
        			}
        			coreSubSequences.add(new LabelSubSequence(subIndex, 1, labels.get(labelsIndex++), mainRegexPredicate.getAttributeNames()));
        		}else{
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
        			
        			coreSubSequences.add(coreSubRegex);
//	        		System.out.println(sub.toString());
//	        		System.out.println(reverseSub.toString());
        			
        		}
        		subIndex ++;
        	}
        	numberOfCoreSubRegexes = subIndex;
        }
        
        if(labels != null && ! labels.isEmpty()){
        	mainRegex = null;
        }else{
        	mainRegex = new SubRegex(mainRegexPredicate, 0, numberOfCoreSubRegexes, mainRegexComplexity);
        	SubRegex mainReverseRegex = new SubRegex(mainReverseRegexPredicate, 
        			0, numberOfCoreSubRegexes, mainRegexComplexity);
        	mainRegex.setReverseSubRegex(mainReverseRegex);
        	if(coreSubSequences.isEmpty()){
        		coreSubSequences.add(mainRegex);
        	}
        	mainRegex.setOriginalSubCount(numberOfCoreSubRegexes);
        }
        
        for(SubSequence sub : coreSubSequences){
        	sub.setOriginalSubCount(numberOfCoreSubRegexes);
        }
        if(numberOfCoreSubRegexes != coreSubSequences.size()){
        	System.out.println("Something wrong. The content of coreSubRegexes is not as expected."); // TODO maybe never happens 
        }
        System.out.println("---------------------------------------");
        for(SubSequence sub: coreSubSequences){
        	System.out.println(sub.toString());
        }
        System.out.println("---------------------------------------");
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
    
    
    // Initializes the subRegexContainer structure with all possible expansions of 
    // non-high sub-regexes
    private void generateExpandedSubRegexes(){
    	
    	// initialize the subRegex container
    	for(int startingCSRIndex = 0 ; startingCSRIndex < coreSubSequences.size(); ++startingCSRIndex){
    		subRegexContainer.add(new ArrayList<>());
    	}
    	// If it's high:
    	// 				make the longest possible sub-regex starting from here ending before the next non-high sub-regex
    	// If it's non-high:
    	// 				just put the core sub-regex on that position and go to the next iteration
    	// Note: Non-high and high sub-regexes are not mixed.
    	for(int startingCSRIndex = 0 ; startingCSRIndex < coreSubSequences.size(); ++startingCSRIndex){
    		
    		SubSequence coreSubSeq = coreSubSequences.get(startingCSRIndex);
    		if(! coreSubSeq.isSubRegex()){
    			subRegexContainer.get(startingCSRIndex).add(coreSubSeq);
    			continue;
    		}
    		SubRegex coreSubRegex = (SubRegex)coreSubSeq;
    		RegexPredicate coreRegexPredicate = coreSubRegex.regex;
    		
    		if(coreSubRegex.complexity != SubRegex.ComplexityLevel.High){
        		if(coreSubRegex.isFirstSubSequence() && coreSubRegex.isLastSubSequence()){
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
    		for(; endingCSRIndex < coreSubSequences.size(); ++endingCSRIndex){
    			if(! coreSubSequences.get(endingCSRIndex).isSubRegex()){
    				break;
    			}
    			SubRegex endingCoreSubRegex = (SubRegex)(coreSubSequences.get(endingCSRIndex));
    			if(endingCoreSubRegex.complexity != SubRegex.ComplexityLevel.High){
    				break;
    			}
    			
    			if(startingCSRIndex != endingCSRIndex){
    				// expandingPredicate => R1R2
    				expandingPredicate.setRegex(expandingPredicate.getRegex() + endingCoreSubRegex.regex.getRegex());
    				expandingPredicate.setSpanListName(expandingPredicate.getSpanListName() + endingCSRIndex);
    				// reverseExpandingPredicate => rev(R2)rev(R1)
    				reverseExpandingPredicate.setRegex(endingCoreSubRegex.getReverseSubRegex().regex.getRegex() + reverseExpandingPredicate.getRegex());
    				reverseExpandingPredicate.setSpanListName(reverseExpandingPredicate.getSpanListName() + endingCSRIndex);
    			}
    		}
    		RegexPredicate newSubRegexPredicate = new RegexPredicate(expandingPredicate);
    		SubRegex newSubRegex = new SubRegex(newSubRegexPredicate, startingCSRIndex, endingCSRIndex - startingCSRIndex, expandingComplexityLevel);
    		newSubRegex.setOriginalSubCount(coreSubSequences.size());
    		// Set the reverse sub regex of the new expanded subregex
    		if(newSubRegex.isFirstSubSequence() && newSubRegex.isLastSubSequence()){
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
    	
    	if(mainRegex != null){
    		// Check the longest sub-regex starting from zero
    		SubSequence sub = subRegexContainer.get(0).get(subRegexContainer.get(0).size() - 1);
    		if(! (sub.isFirstSubSequence() && sub.isLastSubSequence())){ // the regex was actually broken.
    			// the large complete regex which starts from zero and goes to the end
    			subRegexContainer.get(0).add(mainRegex);
    		}
    	}
    	// Prepare query plans
    	if(mainRegex != null){ // it's not a case of having labels
    		queryPlans.add(new SingleRegexPlan(mainRegex));
    	}
    	if(coreSubSequences.size() > 1){
    		List<SubSequence> subSequenceCover = new ArrayList<>();
    		for(int start = 0 ; start < subRegexContainer.size(); start ++){
    			SubSequence next = subRegexContainer.get(start).get(0);
    			subSequenceCover.add(next);
    			start = next.getEnd() - 1;
    		}
    		if(subSequenceCover.size() > 1){
    			queryPlans.add(new MultiRegexPlan(subSequenceCover));
    		}
    	}
    }
    
    
    
    private void printAllSubRegexes(){
    	System.out.println("Core SubRegexes:");
    	for(SubSequence core: coreSubSequences){
    		System.out.println(core.toString());
    		if(core.isSubRegex()){
    			System.out.println(((SubRegex)core).getReverseSubRegex().toString());
    		}
    	}
    	System.out.println("-----------------------------------------------------------------------------------------------------");
    	for(int i =0 ; i < subRegexContainer.size(); ++i){
    		System.out.println("SubRegexes starting at " + i);
    		for(SubSequence sub: subRegexContainer.get(i)){
    			System.out.println(sub.toString());
        		if(sub.isSubRegex()){
        			System.out.println(((SubRegex)sub).getReverseSubRegex().toString());
        		}
    		}
    		System.out.println("-----------------------------------------------------------------------------------------------------");
    	}
    }
    

}