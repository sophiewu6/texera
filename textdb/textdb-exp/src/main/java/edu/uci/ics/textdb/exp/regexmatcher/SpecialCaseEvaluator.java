package edu.uci.ics.textdb.exp.regexmatcher;

import edu.uci.ics.textdb.api.constants.SchemaConstants;
import edu.uci.ics.textdb.api.exception.DataFlowException;
import edu.uci.ics.textdb.api.exception.TextDBException;
import edu.uci.ics.textdb.api.field.ListField;
import edu.uci.ics.textdb.api.schema.AttributeType;
import edu.uci.ics.textdb.api.schema.Schema;
import edu.uci.ics.textdb.api.span.Span;
import edu.uci.ics.textdb.api.tuple.Tuple;
import edu.uci.ics.textdb.exp.common.AbstractSingleInputOperator;
import jersey.repackaged.com.google.common.collect.Lists;
import jersey.repackaged.com.google.common.collect.Sets;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by junm5 on 5/23/17.
 */
public class SpecialCaseEvaluator extends AbstractSingleInputOperator {

    private final RegexPredicate predicate;
    private static final String labelSyntax = "<[^<>]*>";
    private final Set<Character> specailCh = Sets.newHashSet('.', '*', '.', '{', '}');
    private Schema inputSchema;

    public SpecialCaseEvaluator(RegexPredicate predicate) {
        this.predicate = predicate;
    }

    public boolean isValid(String regex, Set<String> labels) {
        return true;
    }

    //This <drug> can <cure> this sea
    public void evaluate(String content, Set<Integer> res, String attributeName, String regex, List<String> labs, int index, Map<String, Set<Span>> labelSpan) {
        if (index == labs.size()) {
            return;
        }
        String label = labs.get(index);
        int i = regex.indexOf(label);
        String head = regex.substring(0, i - 1);
        int len = head.length();
        Set<Span> spens = labelSpan.get(label);
        for (Span span : spens) {
            int start = span.getStart();
            if (start - len < 0) {
                continue;
            }
            String temp = content.substring(start - len, start);
            if (temp.equals(head)) {
                res.add(start - len);
            }
        }
    }

    private List<String> labels(String regex) {
        Pattern pattern = Pattern.compile(labelSyntax, Pattern.CASE_INSENSITIVE);
        Matcher match = pattern.matcher(regex);
        List<String> labels = Lists.newArrayList();
        while (match.find()) {
            int start = match.start();
            int end = match.end();
            labels.add(regex.substring(start + 1, end - 1));
        }
        return labels;
    }

    public static Map<String, Set<Span>> extractLabelSpan(Tuple tuple, Set<String> labels) {
        Map<String, Set<Span>> res = new HashMap<>();
        for (String label : labels) {
            HashSet<Span> values = new HashSet<>();
            res.put(label, values);
            //extract labels from
            ListField<Span> spanListField = tuple.getField(label);
            List<Span> spanList = spanListField.getValue();
            for (Span span : spanList) {
                values.add(span);
            }
        }
        return res;
    }

    private Tuple execute(Tuple inputTuple) {
        List<Span> matchingResults = new ArrayList<>();

        for (String attributeName : predicate.getAttributeNames()) {
            AttributeType attributeType = inputSchema.getAttribute(attributeName).getAttributeType();
            String fieldValue = inputTuple.getField(attributeName).getValue().toString();

            // types other than TEXT and STRING: throw Exception for now
            if (attributeType != AttributeType.STRING && attributeType != AttributeType.TEXT) {
                throw new DataFlowException("KeywordMatcher: Fields other than STRING and TEXT are not supported yet");
            }
//            matchingResults.addAll(javaRegexMatch(fieldValue, attributeName));

        }

        if (matchingResults.isEmpty()) {
            return null;
        }

        ListField<Span> spanListField = inputTuple.getField(SchemaConstants.SPAN_LIST);
        List<Span> spanList = spanListField.getValue();
        spanList.addAll(matchingResults);
        return inputTuple;
    }

    @Override
    protected void setUp() throws TextDBException {
        inputSchema = inputOperator.getOutputSchema();
    }

    @Override
    protected Tuple computeNextMatchingTuple() throws TextDBException {
        return null;
    }

    @Override
    public Tuple processOneInputTuple(Tuple inputTuple) throws TextDBException {
        return null;
    }

    @Override
    protected void cleanUp() throws TextDBException {

    }
}
