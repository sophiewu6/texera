package edu.uci.ics.textdb.exp.regexmatcher.label;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import edu.uci.ics.textdb.api.field.ListField;
import edu.uci.ics.textdb.api.span.Span;
import edu.uci.ics.textdb.api.tuple.Tuple;
import edu.uci.ics.textdb.exp.regexmatcher.RegexMatcher;
import edu.uci.ics.textdb.exp.regexmatcher.RegexPredicate;

/**
 * Helper class for processing labeled regex without any qualifiers.
 * For regex format "this <label1> can cure <label2> better", extract the labels and affixes.
 * affixList {"this "," can cure "," better"}
 * labelList {"label1", "label2"}
 * Sort the affixList in length-decreasing order to select valid tuples with all the affixes inside to short-cut processing time.
 * Generate map of labels and the corresponding spans.
 * For each valid input tuple, match the first label's spans to its prefix and suffix with start/end position information.
 * Filter out the valid spans to continue match with following labels.
 * Early break if no matching in any step.
 *
 * @author Chang Liu
 * @author Zuozhi Wang
 *
 */
public class LabledRegexNoQualifierProcessor {

    private RegexPredicate predicate;
    
    private ArrayList<String> labelList = new ArrayList<>();
    private ArrayList<String> affixList = new ArrayList<>();
    
    class ScoredString{
    	public String value;
    	public int score;
    	ScoredString(String value_){
    		this.value = value_;
    		this.score = 0;
    	}
    }
    
    private ArrayList<ScoredString> scoredAffixList = new ArrayList<>(); // sort the affixList by length in decreasing order to short-cut the filter tuple operation.
    private int tupleStatsUsed = 0;
    private final static int TOTAL_USED_FOR_STATS = 1000;
    
    public LabledRegexNoQualifierProcessor(RegexPredicate predicate) {
        this.predicate = predicate;
        // populate labelList and affixList
        preprocessRegex();
    }

    /**
     * For regex format "this <label1> can cure <label2> better", extract the labels and affixes.
     * affixList {"this "," can cure "," better"}
     * labelList {"label1", "label2"}
     * Sort the affixList in length decreasing order to filter tuples.
     */
    private void preprocessRegex() {
        Matcher labelMatcher = Pattern.compile(RegexMatcher.CHECK_REGEX_LABEL).matcher(predicate.getRegex());
        int pre = 0;
        while (labelMatcher.find()) {
            int start = labelMatcher.start();
            int end = labelMatcher.end();
            
            affixList.add(predicate.getRegex().substring(pre, start));
            labelList.add(predicate.getRegex().substring(
                    labelMatcher.start() + 1, labelMatcher.end() - 1).trim());

            pre = end;
        }
        affixList.add(predicate.getRegex().substring(pre));
        
        scoredAffixList = new ArrayList<>(affixList.stream().map(a -> new ScoredString(a)).collect(Collectors.toList()));
    }

    /**
     * Sort the affixList in length decreasing order to filter valid tuples.
     * @param tuple
     * @param attribute
     * @return
     */
    private boolean filterTuple(Tuple tuple, String attribute, int tupleNumber) {
    	if(tupleNumber <= TOTAL_USED_FOR_STATS){
    		boolean isValid = false;
    		for (ScoredString affix : scoredAffixList) {
    			if (! tuple.getField(attribute).getValue().toString().contains(affix.value)) {
    				isValid = false;
    			}else{
    				affix.score ++;
    			}
    		}
    		
    		if(tupleNumber == TOTAL_USED_FOR_STATS){
    			scoredAffixList = new ArrayList<>(scoredAffixList.stream().sorted((a1, a2) -> a2.score - a1.score).collect(Collectors.toList()));
    		}
    		
    		return isValid;
    	}else{
    		for (ScoredString affix : scoredAffixList) {
    			if (! tuple.getField(attribute).getValue().toString().contains(affix.value)) {
    				return false;
    			}
    		}
    		return true;
    	}
    }

    /***
     * Filter out valid tuple using affixes in the regex.
     * String Match the entire regex from the first label's spans with its prefix and suffix using span's start and end information.
     * Filter out the valid spans to continue match with following labels.
     * Early break if no matching in any step.
     * @param tuple
     * @return
     */
    public List<Span> computeMatchingResults(Tuple tuple) {

        Map<String, List<Span>> labelValues = fetchLabelSpans(tuple);
        
        List<Span> allAttrsMatchSpans = new ArrayList<>();
        for (String attribute : predicate.getAttributeNames()) {
            boolean isValidTuple = filterTuple(tuple, attribute, tupleStatsUsed++);

            if (! isValidTuple) {
                continue;
            }

            String fieldValue = tuple.getField(attribute).getValue().toString();

            List<List<Integer>> matchList = new ArrayList<>();
            
            for (int i = 0; i < labelList.size(); i++) {
                String label = labelList.get(i);
                String prefix = affixList.get(i);
                String suffix = affixList.get(i+1);
                
                List<Span> relevantSpans = labelValues.get(label).stream()
                        .filter(span -> span.getAttributeName().equals(attribute)).collect(Collectors.toList());
                
                if (i == 0) {
                    List<Span> validSpans = relevantSpans.stream()
                            .filter(span -> span.getStart() >= prefix.length())
                            .filter(span -> fieldValue.substring(span.getStart() - prefix.length(), span.getStart()).equals(prefix))
                            .collect(Collectors.toList());
                    matchList = validSpans.stream()
                            .map(span -> new ArrayList<Integer>(Arrays.asList(span.getStart() - prefix.length(), span.getStart())))
                            .collect(Collectors.toList());
                    relevantSpans = validSpans;
                }
                
                List<List<Integer>> newMatchList = new ArrayList<>();
                
                for (List<Integer> previousMatch : matchList) {
                    for (Span span : relevantSpans) {
                        if (previousMatch.get(1) == span.getStart()
                                && span.getEnd() + suffix.length() <= fieldValue.length() 
                                && fieldValue.substring(span.getEnd(), span.getEnd() + suffix.length()).equals(suffix)) {
                            newMatchList.add(Arrays.asList(previousMatch.get(0), span.getEnd() + suffix.length()));
                        }
                    }
                }
                
                matchList = newMatchList;
                if (matchList.isEmpty()) {
                    break;
                }
            }
            
            // assert that for every match:
            //  start >= 0, and end >= 0, and start <= end
            assert(matchList.stream()
                    .filter(match -> match.get(0) >= 0)
                    .filter(match -> match.get(1) >= 0)
                    .filter(match -> match.get(0) <= match.get(1))
                    .count() == (long) matchList.size());
            
            matchList.stream().forEach(match -> allAttrsMatchSpans.add(
                    new Span(attribute, match.get(0), match.get(1), predicate.getRegex(), fieldValue.substring(match.get(0), match.get(1)))));
        }

        return allAttrsMatchSpans;
    }
    
    
    /**
     * Creates Map of label and corresponding spans
     * @param inputTuple
     * @return 
     */
    private Map<String, List<Span>> fetchLabelSpans(Tuple inputTuple) {
        Map<String, List<Span>> labelSpanMap = new HashMap<>();
        for (String label : this.labelList) {
            ListField<Span> spanListField = inputTuple.getField(label);
            labelSpanMap.put(label, spanListField.getValue());
        }
        return labelSpanMap;
    }

}
