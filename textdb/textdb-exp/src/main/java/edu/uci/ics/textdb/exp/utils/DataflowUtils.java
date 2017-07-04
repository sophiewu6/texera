package edu.uci.ics.textdb.exp.utils;

import java.io.IOException;
import java.io.StringReader;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.analysis.util.CharArraySet;
import org.apache.lucene.analysis.standard.StandardAnalyzer;

import edu.uci.ics.textdb.api.constants.SchemaConstants;
import edu.uci.ics.textdb.api.exception.DataFlowException;
import edu.uci.ics.textdb.api.field.DateField;
import edu.uci.ics.textdb.api.field.DoubleField;
import edu.uci.ics.textdb.api.field.IDField;
import edu.uci.ics.textdb.api.field.IField;
import edu.uci.ics.textdb.api.field.IntegerField;
import edu.uci.ics.textdb.api.field.ListField;
import edu.uci.ics.textdb.api.field.StringField;
import edu.uci.ics.textdb.api.field.TextField;
import edu.uci.ics.textdb.api.schema.Attribute;
import edu.uci.ics.textdb.api.schema.AttributeType;
import edu.uci.ics.textdb.api.schema.Schema;
import edu.uci.ics.textdb.api.span.Span;
import edu.uci.ics.textdb.api.tuple.*;
import edu.uci.ics.textdb.storage.constants.LuceneAnalyzerConstants;

public class DataflowUtils {
    
    /**
     * Returns the AttributeType of a field object.
     * 
     * @param field
     * @return
     */
    public static AttributeType getAttributeType(IField field) {
        if (field instanceof DateField) {
            return AttributeType.DATE;
        } else if (field instanceof DoubleField) {
            return AttributeType.DOUBLE;
        } else if (field instanceof IDField) {
            return AttributeType._ID_TYPE;
        } else if (field instanceof IntegerField) {
            return AttributeType.INTEGER;
        } else if (field instanceof ListField) {
            return AttributeType.LIST;
        } else if (field instanceof StringField) {
            return AttributeType.STRING;
        } else if (field instanceof TextField) {
            return AttributeType.TEXT;
        } else {
            throw new RuntimeException("no existing type mapping of this field object");
        }
    }

    /**
     * @about Creating a new span tuple from span schema, field list
     */
    public static Tuple getSpanTuple(List<IField> fieldList, List<Span> spanList, Schema spanSchema) {
        IField spanListField = new ListField<Span>(new ArrayList<>(spanList));
        List<IField> fieldListDuplicate = new ArrayList<>(fieldList);
        fieldListDuplicate.add(spanListField);

        IField[] fieldsDuplicate = fieldListDuplicate.toArray(new IField[fieldListDuplicate.size()]);
        return new Tuple(spanSchema, fieldsDuplicate);
    }
    
    public static ArrayList<String> tokenizeQuery(String luceneAnalyzerStr, String query) {
        try {
            return tokenizeQuery(LuceneAnalyzerConstants.getLuceneAnalyzer(luceneAnalyzerStr), query);
        } catch (DataFlowException e) {
            // TODO: discuss RuntimeException vs. Checked Exception
            throw new RuntimeException(e);
        }
    }
    
    /**
     * Tokenizes the query string using the given analyser
     * 
     * @param luceneAnalyzer
     * @param query
     * @return ArrayList<String> list of results
     */
    public static ArrayList<String> tokenizeQuery(Analyzer luceneAnalyzer, String query) {
        ArrayList<String> result = new ArrayList<String>();
        TokenStream tokenStream = luceneAnalyzer.tokenStream(null, new StringReader(query));
        CharTermAttribute term = tokenStream.addAttribute(CharTermAttribute.class);

        try {
            tokenStream.reset();
            while (tokenStream.incrementToken()) {
                result.add(term.toString());
            }
            tokenStream.close();
        } catch (Exception e) {
            e.printStackTrace();
        }

        return result;
    }

    public static ArrayList<String> tokenizeQueryWithStopwords(String query) {
        ArrayList<String> result = new ArrayList<String>();
        CharArraySet emptyStopwords = new CharArraySet(1, true);
        Analyzer luceneAnalyzer = new StandardAnalyzer(emptyStopwords);
        TokenStream tokenStream = luceneAnalyzer.tokenStream(null, new StringReader(query));
        CharTermAttribute term = tokenStream.addAttribute(CharTermAttribute.class);

        try {
            tokenStream.reset();
            while (tokenStream.incrementToken()) {
                String token = term.toString();
                int tokenIndex = query.toLowerCase().indexOf(token);
                // Since tokens are converted to lower case,
                // get the exact token from the query string.
                String actualQueryToken = query.substring(tokenIndex, tokenIndex + token.length());
                result.add(actualQueryToken);
            }
            tokenStream.close();
        } catch (Exception e) {
            e.printStackTrace();
        }

        luceneAnalyzer.close();

        return result;
    }
    
    public static String getTupleListString(List<Tuple> tupleList) {
        StringBuilder sb = new StringBuilder();
        for (Tuple tuple : tupleList) {
            sb.append(getTupleString(tuple));
            sb.append("\n");
        }
        return sb.toString();
    }

    /**
     * Transform a tuple into string
     * 
     * @param tuple
     * @return string representation of the tuple
     */
    public static String getTupleString(Tuple tuple) {
        StringBuilder sb = new StringBuilder();

        Schema schema = tuple.getSchema();
        for (Attribute attribute : schema.getAttributes()) {
            if (attribute.getAttributeName().equals(SchemaConstants.SPAN_LIST)) {
                ListField<Span> spanListField = tuple.getField(SchemaConstants.SPAN_LIST);
                List<Span> spanList = spanListField.getValue();
                sb.append(getSpanListString(spanList));
                sb.append("\n");
            } else {
                sb.append(attribute.getAttributeName());
                sb.append("(");
                sb.append(attribute.getAttributeType().toString());
                sb.append(")");
                sb.append(": ");
                sb.append(tuple.getField(attribute.getAttributeName()).getValue().toString());
                sb.append("\n");
            }
        }

        return sb.toString();
    }

    /**
     * Transform a list of spans into string
     * 
     * @param spanList
     * @return string representation of a list of spans
     */

    public static String getSpanListString(List<Span> spanList) {
        StringBuilder sb = new StringBuilder();

        sb.append("span list:\n");
        for (Span span : spanList) {
            sb.append(getSpanString(span));
            sb.append("\n");
        }

        return sb.toString();
    }

    /**
     * Transform a span into string
     * 
     * @param span
     * @return string representation of a span
     */

    public static String getSpanString(Span span) {
        StringBuilder sb = new StringBuilder();

        sb.append("field: " + span.getAttributeName() + "\n");
        sb.append("start: " + span.getStart() + "\n");
        sb.append("end:   " + span.getEnd() + "\n");
        sb.append("key:   " + span.getKey() + "\n");
        sb.append("value: " + span.getValue() + "\n");
        sb.append("token offset: " + span.getTokenOffset() + "\n");

        return sb.toString();
    }
    
    public static List<Span> generatePayloadFromTuple(Tuple tuple, String luceneAnalyzer) throws DataFlowException {
        return generatePayloadFromTuple(tuple, LuceneAnalyzerConstants.getLuceneAnalyzer(luceneAnalyzer));
    }

    public static List<Span> generatePayloadFromTuple(Tuple tuple, Analyzer luceneAnalyzer) {
        List<Span> tuplePayload = tuple.getSchema().getAttributes().stream()
                .filter(attr -> (attr.getAttributeType() == AttributeType.TEXT)) // generate payload only for TEXT field
                .map(attr -> attr.getAttributeName())
                .map(attributeName -> generatePayload(attributeName, tuple.getField(attributeName).getValue().toString(),
                        luceneAnalyzer))
                .flatMap(payload -> payload.stream()) // flatten a list of lists to a list
                .collect(Collectors.toList());

        return tuplePayload;
    }

    public static List<Span> generatePayload(String attributeName, String fieldValue, Analyzer luceneAnalyzer) {
        List<Span> payload = new ArrayList<>();
        
        try {
            TokenStream tokenStream = luceneAnalyzer.tokenStream(null, new StringReader(fieldValue));
            OffsetAttribute offsetAttribute = tokenStream.addAttribute(OffsetAttribute.class);
            CharTermAttribute charTermAttribute = tokenStream.addAttribute(CharTermAttribute.class);
            PositionIncrementAttribute positionIncrementAttribute = 
                    tokenStream.addAttribute(PositionIncrementAttribute.class);
            
            int tokenPositionCounter = -1;
            tokenStream.reset();
            while (tokenStream.incrementToken()) {
                tokenPositionCounter += positionIncrementAttribute.getPositionIncrement();
                
                int tokenPosition = tokenPositionCounter;
                int charStart = offsetAttribute.startOffset();
                int charEnd = offsetAttribute.endOffset();
                String analyzedTermStr = charTermAttribute.toString();
                String originalTermStr = fieldValue.substring(charStart, charEnd);

                payload.add(new Span(attributeName, charStart, charEnd, analyzedTermStr, originalTermStr, tokenPosition));
            }
            tokenStream.close();
        } catch (IOException e) {
            payload.clear(); // return empty payload
        }

        return payload;
    }

    /***
     *
     * @param inputTuple
     * @param attributeNames
     * @param queryKeyword
     * @param matchingResults
     * @throws DataFlowException
     */
    public static void appendSubstringMatchingSpans(Tuple inputTuple, List<String> attributeNames, String queryKeyword, List<Span> matchingResults) throws DataFlowException {


        for (String attributeName : attributeNames) {
            //  AttributeType attributeType = this.inputSchema.getAttribute(attributeName).getAttributeType();
            String fieldValue = inputTuple.getField(attributeName).getValue().toString();
            AttributeType attributeType = inputTuple.getSchema().getAttribute(attributeName).getAttributeType();
            // types other than TEXT and STRING: throw Exception for now
            if (attributeType != AttributeType.STRING && attributeType != AttributeType.TEXT) {
                throw new DataFlowException("KeywordMatcher: Fields other than STRING and TEXT are not supported yet");
            }

            // for STRING type, the query should match the fieldValue completely
            if (attributeType == AttributeType.STRING) {
                if (fieldValue.equals(queryKeyword)) {
                    matchingResults.add(new Span(attributeName, 0, queryKeyword.length(), queryKeyword, fieldValue));
                }
            }

            if (attributeType == AttributeType.TEXT) {
                
                String fieldValueLowerCase = fieldValue.toLowerCase();
                String queryKeywordLowerCase = queryKeyword.toLowerCase();
                for(int i = 0 ; i < fieldValueLowerCase.length(); i++){
                	int index = -1;
                	if((index = fieldValueLowerCase.indexOf(queryKeywordLowerCase,i)) != -1){
                		matchingResults.add(new Span(attributeName, index, index + queryKeyword.length(), queryKeyword, 
                				fieldValue.substring(index, index + queryKeyword.length())));
                		i = index + 1;
                	}else{
                		break;
                	}
                }

            }
        }
    }

    public static void appendConjunctionMatchingSpans(Tuple inputTuple, List<String> attributeNames, String queryKeyword, String luceneAnalyzerString, List<Span> matchingResults) throws DataFlowException {
        ListField<Span> payloadField = inputTuple.getField(SchemaConstants.PAYLOAD);
        List<Span> payload = payloadField.getValue();
        List<String> queryTokenList = tokenizeQuery(luceneAnalyzerString, queryKeyword);
        Set<String> queryTokenSet = new HashSet<>(queryTokenList);
        List<Span> relevantSpans = filterRelevantSpans(payload,queryTokenSet);

        for (String attributeName : attributeNames) {
            AttributeType attributeType = inputTuple.getSchema().getAttribute(attributeName).getAttributeType();
            String fieldValue = inputTuple.getField(attributeName).getValue().toString();

            // types other than TEXT and STRING: throw Exception for now
            if (attributeType != AttributeType.STRING && attributeType != AttributeType.TEXT) {
                throw new DataFlowException("KeywordMatcher: Fields other than STRING and TEXT are not supported yet");
            }

            // for STRING type, the query should match the fieldValue completely
            if (attributeType == AttributeType.STRING) {
                if (fieldValue.equals(queryKeyword)) {
                    Span span = new Span(attributeName, 0, queryKeyword.length(), queryKeyword, fieldValue);
                    matchingResults.add(span);
                }
            }

            // for TEXT type, every token in the query should be present in span
            // list for this field
            if (attributeType == AttributeType.TEXT) {
                List<Span> fieldSpanList = relevantSpans.stream().filter(span -> span.getAttributeName().equals(attributeName))
                        .collect(Collectors.toList());

                if (isAllQueryTokensPresent(fieldSpanList, queryTokenSet)) {
                    matchingResults.addAll(fieldSpanList);
                }
            }
        }
    }

    /***
     *
     * @param inputTuple
     * @param attributeNames
     * @param queryKeyword
     * @param luceneAnalyzerString
     * @param matchingResults
     * @return
     * @throws DataFlowException
     */
    public static void appendPhraseMatchingSpans(Tuple inputTuple, List<String> attributeNames, String queryKeyword, String luceneAnalyzerString, List<Span> matchingResults) throws DataFlowException {
        ListField<Span> payloadField = inputTuple.getField(SchemaConstants.PAYLOAD);
        List<Span> payload = payloadField.getValue();
        List<String> queryTokenList = tokenizeQuery(luceneAnalyzerString, queryKeyword);
        Set<String> queryTokenSet = new HashSet<>(queryTokenList);
        List<Span> relevantSpans = filterRelevantSpans(payload, queryTokenSet);
        List<String> queryTokensWithStopwords = DataflowUtils.tokenizeQueryWithStopwords(queryKeyword);

        for (String attributeName : attributeNames) {
            AttributeType attributeType = inputTuple.getSchema().getAttribute(attributeName).getAttributeType();
            String fieldValue = inputTuple.getField(attributeName).getValue().toString();

            // types other than TEXT and STRING: throw Exception for now
            if (attributeType != AttributeType.STRING && attributeType != AttributeType.TEXT) {
                throw new DataFlowException("KeywordMatcher: Fields other than STRING and TEXT are not supported yet");
            }

            // for STRING type, the query should match the fieldValue completely
            if (attributeType == AttributeType.STRING) {
                if (fieldValue.equals(queryKeyword)) {
                    matchingResults.add(new Span(attributeName, 0, queryKeyword.length(), queryKeyword, fieldValue));
                }
            }

            // for TEXT type, spans need to be reconstructed according to the
            // phrase query
            if (attributeType == AttributeType.TEXT) {
                List<Span> fieldSpanList = relevantSpans.stream().filter(span -> span.getAttributeName().equals(attributeName))
                        .collect(Collectors.toList());

                if (!isAllQueryTokensPresent(fieldSpanList, queryTokenSet)) {
                    // move on to next field if not all query tokens are present
                    // in the spans
                    continue;
                }

                // Sort current field's span list by token offset for later use
                Collections.sort(fieldSpanList, (span1, span2) -> span1.getTokenOffset() - span2.getTokenOffset());

                List<Integer> queryTokenOffset = new ArrayList<>();

                for (int i = 0; i < queryTokensWithStopwords.size(); i++) {
                    if (queryTokenList.contains(queryTokensWithStopwords.get(i))) {
                        queryTokenOffset.add(i);
                    }
                }

                int iter = 0; // maintains position of term being checked in
                // spanForThisField list
                while (iter < fieldSpanList.size()) {
                    if (iter > fieldSpanList.size() - queryTokenList.size()) {
                        break;
                    }

                    // Verify if span in the spanForThisField correspond to our
                    // phrase query, ie relative position offsets should be
                    // similar
                    // and the value should be same.
                    boolean isMismatchInSpan = false;// flag to check if a
                    // mismatch in spans occurs

                    // To check all the terms in query are verified
                    for (int i = 0; i < queryTokenList.size() - 1; i++) {
                        Span first = fieldSpanList.get(iter + i);
                        Span second = fieldSpanList.get(iter + i + 1);
                        if (!(second.getTokenOffset() - first.getTokenOffset() == queryTokenOffset.get(i + 1)
                                - queryTokenOffset.get(i) && first.getValue().equalsIgnoreCase(queryTokenList.get(i))
                                && second.getValue().equalsIgnoreCase(queryTokenList.get(i + 1)))) {
                            iter++;
                            isMismatchInSpan = true;
                            break;
                        }
                    }

                    if (isMismatchInSpan) {
                        continue;
                    }

                    int combinedSpanStartIndex = fieldSpanList.get(iter).getStart();
                    int combinedSpanEndIndex = fieldSpanList.get(iter + queryTokenList.size() - 1).getEnd();

                    Span combinedSpan = new Span(attributeName, combinedSpanStartIndex, combinedSpanEndIndex, queryKeyword,
                            fieldValue.substring(combinedSpanStartIndex, combinedSpanEndIndex));
                    matchingResults.add(combinedSpan);
                    iter = iter + queryTokenList.size();
                }
            }
        }

    }

    private static boolean isAllQueryTokensPresent(List<Span> fieldSpanList, Set<String> queryTokenSet) {
        Set<String> fieldSpanKeys = fieldSpanList.stream().map(span -> span.getKey()).collect(Collectors.toSet());

        return fieldSpanKeys.equals(queryTokenSet);
    }

    private static List<Span> filterRelevantSpans(List<Span> spanList, Set<String> queryTokenSet) {
        List<Span> relevantSpans = new ArrayList<>();
        Iterator<Span> iterator = spanList.iterator();
        while (iterator.hasNext()) {
            Span span = iterator.next();
            if (queryTokenSet.contains(span.getKey())) {
                relevantSpans.add(span);
            }
        }
        return relevantSpans;
    }


}
