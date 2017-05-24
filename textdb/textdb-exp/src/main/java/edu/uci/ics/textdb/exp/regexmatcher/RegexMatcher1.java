package edu.uci.ics.textdb.exp.regexmatcher;

import edu.uci.ics.textdb.api.constants.SchemaConstants;
import edu.uci.ics.textdb.api.exception.DataFlowException;
import edu.uci.ics.textdb.api.exception.TextDBException;
import edu.uci.ics.textdb.api.field.ListField;
import edu.uci.ics.textdb.api.schema.AttributeType;
import edu.uci.ics.textdb.api.schema.Schema;
import edu.uci.ics.textdb.api.span.Span;
import edu.uci.ics.textdb.api.tuple.Tuple;
import edu.uci.ics.textdb.api.utils.Utils;
import edu.uci.ics.textdb.exp.common.AbstractSingleInputOperator;
import edu.uci.ics.textdb.exp.utils.DataflowUtils;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by chenli on 3/25/16.
 *
 * @author Shuying Lai (laisycs)
 * @author Zuozhi Wang (zuozhiw)
 */
public class RegexMatcher1 extends AbstractSingleInputOperator {

    private final RegexPredicate predicate;
    private static final String labelSyntax = "<[^<>]*>";
    //(<[^<>]*>[+]*)

    // two available regex engines, RegexMatcher will try RE2J first
    private enum RegexEngine {
        JavaRegex, RE2J
    }

    private Map<Integer, Set<String>> idLabelMapping;
    private Map<Integer, String> suffixMapping;
    private String regexMod;

    private Schema inputSchema;

    public RegexMatcher1(RegexPredicate predicate) {
        this.predicate = predicate;
    }

    @Override
    protected void setUp() throws DataFlowException {
        inputSchema = inputOperator.getOutputSchema();
        outputSchema = inputSchema;
        idLabelMapping = new HashMap<>();
        suffixMapping = new HashMap<>();
        regexMod = extractLabels(predicate.getRegex(), idLabelMapping, suffixMapping);

        if (!this.inputSchema.containsField(SchemaConstants.SPAN_LIST)) {
            outputSchema = Utils.createSpanSchema(inputSchema);
        }
    }


    @Override
    protected Tuple computeNextMatchingTuple() throws TextDBException {
        Tuple inputTuple = null;
        Tuple resultTuple = null;

        while ((inputTuple = inputOperator.getNextTuple()) != null) {
            if (!inputSchema.containsField(SchemaConstants.SPAN_LIST)) {
                inputTuple = DataflowUtils.getSpanTuple(inputTuple.getFields(), new ArrayList<Span>(), outputSchema);
            }
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
     * @param inputTuple document in which search is performed
     * @return a list of spans describing the occurrence of a matching sequence
     * in the document
     * @throws DataFlowException
     */
    @Override
    public Tuple processOneInputTuple(Tuple inputTuple) {


        if (inputTuple == null) {
            return null;
        }

        return processLabelledRegex(inputTuple);

    }

    private Tuple processLabelledRegex(Tuple inputTuple) {
        //Map<Integer, String> suffix = generateSuffix(regexMod,labelSyntax);
        Map<Integer, List<Span>> labelSpanList = createLabelledSpanList(inputTuple, idLabelMapping);
        List<Span> spen = labelledRegexMatcher(inputTuple, labelSpanList, suffixMapping);

        if (spen.isEmpty()) {
            return null;
        }

        ListField<Span> spanListField1 = inputTuple.getField(SchemaConstants.SPAN_LIST);
        List<Span> spanList1 = spanListField1.getValue();
        spanList1.addAll(spen);

        return inputTuple;
    }


    @Override
    protected void cleanUp() throws DataFlowException {
    }

    public RegexPredicate getPredicate() {
        return this.predicate;
    }

    private String extractLabels(String generalRegexPattern, Map<Integer, Set<String>> idLabelMapping, Map<Integer, String> suffixMapping) {
        Pattern pattern = Pattern.compile(labelSyntax, Pattern.CASE_INSENSITIVE);
        Matcher match = pattern.matcher(generalRegexPattern);
        int key = 0;
        int pre = 0;
        int id = 1;
        String regexMod = generalRegexPattern;
        while (match.find()) {
            int start = match.start();
            int end = match.end();
            String suffix = generalRegexPattern.substring(pre, start);
            suffixMapping.put(key, suffix);
            String substr = generalRegexPattern.substring(start + 1, end - 1);
            String substrWithoutSpace = substr.replaceAll("\\s+", "");

            idLabelMapping.put(id, new HashSet<String>());

            if (substrWithoutSpace.contains("|")) {
                // Multiple value separated by OR operator
                String[] sublabs = substrWithoutSpace.split("[|]");
                for (String lab : sublabs)
                    idLabelMapping.get(id).add(lab);
            } else {
                idLabelMapping.get(id).add(substrWithoutSpace);
            }
            regexMod = regexMod.replace("<" + substr + ">", "<" + id + ">");
            id++;
            key++;
            pre = end;
        }
        suffixMapping.put(key, generalRegexPattern.substring(pre));
        return regexMod;
    }

    private List<Span> labelledRegexMatcher(Tuple inputTuple, Map<Integer, List<Span>> labelIDSpanMap, Map<Integer, String> suffixMap) throws DataFlowException {
        if (inputTuple == null) {
            return null;
        }
        List<Span> matchingResults = new ArrayList<>();

        for (String attributeName : predicate.getAttributeNames()) {
            AttributeType attributeType = inputSchema.getAttribute(attributeName).getAttributeType();
            String fieldValue = inputTuple.getField(attributeName).getValue().toString();

            // types other than TEXT and STRING: throw Exception for now
            if (attributeType != AttributeType.STRING && attributeType != AttributeType.TEXT) {
                throw new DataFlowException("KeywordMatcher: Fields other than STRING and TEXT are not supported yet");
            }

            Map<Integer, Set<Integer>> integerSetMap = genMatchSuffix(fieldValue, attributeName, suffixMap);
            List<int[]> indexs = generateAllCombinationsOfRegex(attributeName, suffixMap, labelIDSpanMap, integerSetMap);
            for (int[] entry : indexs) {
                String spanValue = fieldValue.substring(entry[0], entry[1]);
                matchingResults.add(new Span(attributeName, entry[0], entry[1], predicate.getRegex(), spanValue));
            }
        }

        return matchingResults;
    }

    private Map<Integer, Set<Integer>> genMatchSuffix(String fieldValue, String attributeName, Map<Integer, String> toMatch) {
        Map<Integer, Set<Integer>> map = new HashMap<>();
        for (Integer key : toMatch.keySet()) {
            if (toMatch.get(key).length() != 0) {
                Set<Integer> set = new HashSet<>();
                Pattern javaPattern = Pattern.compile(toMatch.get(key),
                        Pattern.CASE_INSENSITIVE);
                java.util.regex.Matcher javaMatcher = javaPattern.matcher(fieldValue);
                while (javaMatcher.find()) {
                    int start = javaMatcher.start();
                    set.add(start);
                }
                map.put(key, set);
            }
        }
        return map;
    }

    /***
     *
     * @param suffixMap: regex -- token position except from label
     * @param labelSpanList: label id --- spanlist
     * @param suffixIndex: regex tokens start position
     * @return
     */
    private List<int[]> generateAllCombinationsOfRegex(String attrName, Map<Integer, String> suffixMap, Map<Integer, List<Span>> labelSpanList, Map<Integer, Set<Integer>> suffixIndex) {
        if (suffixMap.get(0).length() != 0) {
            String toMatch = suffixMap.get(0);
            for (Span span : labelSpanList.get(1)) {
                if (!span.getAttributeName().equals(attrName)) {
                    continue;
                }
                int start = span.getStart() - toMatch.length();
                if (!suffixIndex.get(0).contains(start)) {
                    labelSpanList.get(1).remove(span);
                } else {
//                    Span temp= new Span(span.getAttributeName(), start, span.getEnd(), span.getKey(), toMatch + span.getValue());
                    span.setStart(start);
//                    spanRes.add(temp);
                }
            }
        }

        for (int i = 1; i <= labelSpanList.size(); i++) {
            String suffix = suffixMap.get(i);
            if (suffix.length() != 0) {
                for (Span span : labelSpanList.get(i)) {
                    if (!span.getAttributeName().equals(attrName)) {
                        continue;
                    }
                    if (!suffixIndex.get(i).contains(span.getEnd())) {
                        labelSpanList.get(i).remove(span);
                    } else {
                        span.setEnd(span.getEnd() + suffix.length());
                    }
                }
            }
        }

        List<int[]> res = new ArrayList<>();
        for (Span span : labelSpanList.get(1)) {
            int[] iArray = new int[2];
            if (labelSpanList.size() == 1) {
                iArray[0] = span.getStart();
                iArray[1] = span.getEnd();
            } else {
                iArray[0] = span.getStart();
                iArray[1] = helper(span, labelSpanList, 2)[1];
            }
            if (iArray[1] != -1) {
                res.add(iArray);
            }
        }
        return res;
    }

    private int[] helper(Span span, Map<Integer, List<Span>> labelSpanList, int index) {
        int[] res = new int[2];
        Arrays.fill(res, -1);
        if (index > labelSpanList.size()) {
            return res;
        }
        int start = span.getEnd();
        Span next = binarySearch(labelSpanList.get(index), start);
        if (next.getStart() != -1 && next.getValue() != null) {
            if (index == labelSpanList.size()) {
                res[0] = next.getStart();
                res[1] = next.getEnd();
            } else if (index < labelSpanList.size()) {
                res[0] = next.getStart();
                res[1] = helper(next, labelSpanList, index + 1)[1];
            }
        }

        return res;
    }

    private boolean validCombination(List<Span> list) {
        for (int i = 0; i < list.size() - 1; i++) {
            if (list.get(i + 1).getStart() - list.get(i).getEnd() < 0) {
                return false;
            }
        }
        return true;
    }


    private Map<Integer, List<Span>> createLabelledSpanList(Tuple inputTuple, Map<Integer, Set<String>> idLabelMapping) {
        Map<Integer, List<Span>> labelSpanList = new HashMap<>();
        for (int id : idLabelMapping.keySet()) {
            Set<String> labels = idLabelMapping.get(id);
            List<Span> values = new ArrayList<>();
            //HashSet<String> values = new HashSet<String>();
            for (String oneField : labels) {
                ListField<Span> spanListField = inputTuple.getField(oneField);
                List<Span> spanList = spanListField.getValue();
                values.addAll(spanList);
            }

            labelSpanList.put(id, SortSpanlist(values));
        }
        return labelSpanList;
    }

    private List<Span> SortSpanlist(List<Span> spanList) {
        // ListField<Span> spanListField = inputTuple.getField(s);
        // List<Span> spanList = spanListField.getValue();
        Collections.sort(spanList, new Comparator<Span>() {
            @Override
            public int compare(Span o1, Span o2) {
                return o1.getStart() - o2.getStart();
            }
        });

        return spanList;
    }

    private static Span binarySearch(List<Span> list, int index) {
        int start = 0;
        int end = list.size() - 1;
        while (start <= end) {
            int mid = start + (end - start) / 2;
            if (list.get(mid).getStart() == index) {
                return list.get(mid);
            } else if (list.get(mid).getStart() < index) {
                start = mid + 1;
            } else {
                end = mid - 1;
            }
        }
        Span nul = new Span("nul", -1, -1, "offset", "null");
        return nul;
    }
}
