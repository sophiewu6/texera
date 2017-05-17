package edu.uci.ics.textdb.exp.regexmatcher;

import java.util.*;

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
import jersey.repackaged.com.google.common.base.Joiner;

/**
 * Created by chenli on 3/25/16.
 *
 * @author Shuying Lai (laisycs)
 * @author Zuozhi Wang (zuozhiw)
 * @author Jun Ma (jm)
 */
public class RegexMatcher extends AbstractSingleInputOperator {

    private final RegexPredicate predicate;
    private static final String labelSyntax = "<[^<>]*>";

    // two available regex engines, RegexMatcher will try RE2J first
    private enum RegexEngine {
        JavaRegex, RE2J
    }

    private RegexEngine regexEngine;
    private com.google.re2j.Pattern re2jPattern;
    private java.util.regex.Pattern javaPattern;
    private HashMap<Integer, HashSet<String>> idLabelMapping;
    private Set<String> inputLabels;

    private Schema inputSchema;

    public RegexMatcher(RegexPredicate predicate) {
        this.predicate = predicate;
    }

    @Override
    protected void setUp() throws DataFlowException {
        inputSchema = inputOperator.getOutputSchema();
        outputSchema = inputSchema;
        idLabelMapping = new HashMap<>();
        inputLabels = extractLabels(predicate.getRegex());
        if (idLabelMapping.size() == 0) {
            // No labels in regex
            compileRegexPattern(predicate.getRegex());
        }
        if (!this.inputSchema.containsField(SchemaConstants.SPAN_LIST)) {
            outputSchema = Utils.createSpanSchema(inputSchema);
        }
    }


    @Override
    protected Tuple computeNextMatchingTuple() throws TextDBException {
        Tuple inputTuple;
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
        String newRegex = parser(inputTuple, inputLabels, predicate.getRegex());
        compileRegexPattern(newRegex);
        return execute(inputTuple);
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
            switch (regexEngine) {
                case JavaRegex:
                    matchingResults.addAll(javaRegexMatch(fieldValue, attributeName));
                    break;
                case RE2J:
                    matchingResults.addAll(re2jRegexMatch(fieldValue, attributeName));
                    break;
            }
        }

        if (matchingResults.isEmpty()) {
            return null;
        }

        ListField<Span> spanListField = inputTuple.getField(SchemaConstants.SPAN_LIST);
        List<Span> spanList = spanListField.getValue();
        spanList.addAll(matchingResults);
        return inputTuple;
    }


    private void compileRegexPattern(String regex) {
        // try Java Regex first
        try {
            if (this.predicate.isIgnoreCase()) {
                this.javaPattern = java.util.regex.Pattern.compile(regex,
                        java.util.regex.Pattern.CASE_INSENSITIVE);
                this.regexEngine = RegexEngine.JavaRegex;
            } else {
                this.javaPattern = java.util.regex.Pattern.compile(regex);
                this.regexEngine = RegexEngine.JavaRegex;
            }

            // if Java Regex fails, try RE2J
        } catch (java.util.regex.PatternSyntaxException javaException) {
            try {
                if (this.predicate.isIgnoreCase()) {
                    this.re2jPattern = com.google.re2j.Pattern.compile(regex,
                            com.google.re2j.Pattern.CASE_INSENSITIVE);
                    this.regexEngine = RegexEngine.RE2J;
                } else {
                    this.re2jPattern = com.google.re2j.Pattern.compile(regex);
                    this.regexEngine = RegexEngine.RE2J;
                }

                // if RE2J also fails, throw exception
            } catch (com.google.re2j.PatternSyntaxException re2jException) {
                throw new DataFlowException(javaException.getMessage(), javaException);
            }
        }
    }


    /**
     *
     * @param regex
     * @return
     */
    public static Set<String> extractLabels(String regex) {
        java.util.regex.Pattern pattern = java.util.regex.Pattern.compile(labelSyntax,
                java.util.regex.Pattern.CASE_INSENSITIVE);

        java.util.regex.Matcher match = pattern.matcher(regex);

        Set<String> labels = new HashSet<>();
        while (match.find()) {
            int start = match.start();
            int end = match.end();
            labels.add(regex.substring(start + 1, end - 1));
        }
        return labels;
    }

    public static String parser(Tuple tuple, Set<String> labels, String regex) {
        if (!labels.isEmpty()) {
            Map<String, Set<String>> labelValuesMap = extractLabelValues(tuple, labels);
            for (String label : labels) {
                regex.replace("<" + label + ">", union(labelValuesMap.get(label)));
            }
        }
        return regex;
    }

    public static Map<String, Set<String>> extractLabelValues(Tuple tuple, Set<String> labels) {
        Map<String, Set<String>> res = new HashMap<>();
        for (String label : labels) {
            HashSet<String> values = new HashSet<>();
            res.put(label, values);
            //extract labels from
            ListField<Span> spanListField = tuple.getField(label);
            List<Span> spanList = spanListField.getValue();
            for (Span span : spanList) {
                values.add(span.getValue());
            }
        }
        return res;
    }

    public static String union(Set<String> set) {
        return new StringBuffer().append("(")
                .append(Joiner.on("|").join(set))
                .append(")").toString();
    }

    private List<Span> javaRegexMatch(String fieldValue, String attributeName) {
        List<Span> matchingResults = new ArrayList<>();
        java.util.regex.Matcher javaMatcher = this.javaPattern.matcher(fieldValue);
        while (javaMatcher.find()) {
            int start = javaMatcher.start();
            int end = javaMatcher.end();
            matchingResults.add(
                    new Span(attributeName, start, end, this.predicate.getRegex(), fieldValue.substring(start, end)));
        }
        return matchingResults;
    }

    private List<Span> re2jRegexMatch(String fieldValue, String attributeName) {
        List<Span> matchingResults = new ArrayList<>();
        com.google.re2j.Matcher re2jMatcher = this.re2jPattern.matcher(fieldValue);
        while (re2jMatcher.find()) {
            int start = re2jMatcher.start();
            int end = re2jMatcher.end();
            matchingResults.add(
                    new Span(attributeName, start, end, this.predicate.getRegex(), fieldValue.substring(start, end)));
        }
        return matchingResults;
    }

    @Override
    protected void cleanUp() throws DataFlowException {
    }

    public RegexPredicate getPredicate() {
        return this.predicate;
    }
}
