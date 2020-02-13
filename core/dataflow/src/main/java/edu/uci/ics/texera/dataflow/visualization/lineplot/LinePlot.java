package edu.uci.ics.texera.dataflow.visualization.lineplot;


import java.util.*;
import java.util.stream.Collectors;

import edu.uci.ics.texera.api.constants.ErrorMessages;
import edu.uci.ics.texera.api.constants.SchemaConstants;
import edu.uci.ics.texera.api.exception.DataflowException;
import edu.uci.ics.texera.api.exception.TexeraException;
import edu.uci.ics.texera.api.field.ListField;
import edu.uci.ics.texera.api.schema.AttributeType;
import edu.uci.ics.texera.api.schema.Schema;
import edu.uci.ics.texera.api.span.Span;
import edu.uci.ics.texera.api.tuple.Tuple;
import edu.uci.ics.texera.dataflow.common.AbstractSingleInputOperator;
import edu.uci.ics.texera.dataflow.visualization.lineplot.LinePlotPredicate;
import edu.uci.ics.texera.dataflow.utils.DataflowUtils;

public class LinePlot extends AbstractSingleInputOperator{

    private final LinePlotPredicate predicate;

    private String aggregateAttribute;

    public LinePlot(LinePlotPredicate predicate) {
        this.predicate = predicate;
    }

    @Override
    protected void setUp() throws TexeraException {
        if (inputOperator == null) {
            throw new DataflowException(ErrorMessages.INPUT_OPERATOR_NOT_SPECIFIED);
        }
        this.outputSchema = inputOperator.getOutputSchema();
//        aggregateAttribute = predicate.getXAxis();
    }

    @Override
    protected Tuple computeNextMatchingTuple() throws TexeraException {
        Tuple inputTuple = null;
        Tuple resultTuple = null;

        while ((inputTuple = inputOperator.getNextTuple()) != null) {
            resultTuple = processOneInputTuple(inputTuple);

            if (resultTuple != null) {
                break;
            }
        }
        return resultTuple;
    }

    @Override
    public Tuple processOneInputTuple(Tuple inputTuple) throws TexeraException {
        // add payload if needed before passing it to the matching functions
//        if (addPayload) {
//            Tuple.Builder tupleBuilderPayload = new Tuple.Builder(inputTuple);
//            tupleBuilderPayload.add(SchemaConstants.PAYLOAD_ATTRIBUTE, new ListField<Span>(
//                    DataflowUtils.generatePayloadFromTuple(inputTuple, predicate.getLuceneAnalyzerString())));
//            inputTuple = tupleBuilderPayload.build();
//        }
//
//        // compute the keyword matching results
//        List<Span> matchingResults = null;
//        if (this.predicate.getMatchingType() == KeywordMatchingType.CONJUNCTION_INDEXBASED) {
//            matchingResults =  appendConjunctionMatchingSpans(inputTuple, predicate.getAttributeNames(), queryTokenSet, predicate.getQuery());
//        }
//        if (this.predicate.getMatchingType() == KeywordMatchingType.PHRASE_INDEXBASED) {
//            matchingResults = appendPhraseMatchingSpans(inputTuple, predicate.getAttributeNames(), queryTokenList, queryTokenWithStopwordsList, predicate.getQuery());
//        }
//        if (this.predicate.getMatchingType() == KeywordMatchingType.SUBSTRING_SCANBASED) {
//            matchingResults = appendSubstringMatchingSpans(inputTuple, predicate.getAttributeNames(), predicate.getQuery());
//        }
//
//        if (matchingResults.isEmpty()) {
//            return null;
//        }
//
        Tuple.Builder tupleBuilder = new Tuple.Builder(inputTuple);
//        if (addResultAttribute) {
//            tupleBuilder.add(predicate.getSpanListName(), AttributeType.LIST, new ListField<Span>(matchingResults));
//        }
        return tupleBuilder.build();
    }

    @Override
    protected void cleanUp() {
    }

    public Schema transformToOutputSchema(Schema... inputSchema) {
        if (inputSchema.length != 1)
            throw new TexeraException(String.format(ErrorMessages.NUMBER_OF_ARGUMENTS_DOES_NOT_MATCH, 1, inputSchema.length));
        return inputSchema[0];
    }
}
