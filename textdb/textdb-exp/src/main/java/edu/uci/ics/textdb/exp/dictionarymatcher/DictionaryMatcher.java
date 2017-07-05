package edu.uci.ics.textdb.exp.dictionarymatcher;

import edu.uci.ics.textdb.api.constants.ErrorMessages;
import edu.uci.ics.textdb.api.constants.SchemaConstants;
import edu.uci.ics.textdb.api.dataflow.IOperator;
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
import edu.uci.ics.textdb.exp.keywordmatcher.KeywordMatcher;
import edu.uci.ics.textdb.exp.keywordmatcher.KeywordMatchingType;
import edu.uci.ics.textdb.exp.keywordmatcher.KeywordPredicate;
import edu.uci.ics.textdb.exp.utils.DataflowUtils;

import java.util.ArrayList;
import java.util.List;


/**
 * Created by Chang on 6/28/17.
 */
public class DictionaryMatcher extends AbstractSingleInputOperator {

    private final DictionaryPredicate predicate;


    public DictionaryMatcher(DictionaryPredicate predicate) {
        this.predicate = predicate;
    }

    private Schema inputSchema;

    @Override
    protected void setUp() throws TextDBException {

        if (inputOperator == null) {
            throw new DataFlowException(ErrorMessages.INPUT_OPERATOR_NOT_SPECIFIED);
        }
        inputSchema = inputOperator.getOutputSchema();
        predicate.getDictionary().resetCursor();

        if (predicate.getDictionary().isEmpty()) {
            throw new DataFlowException("Dictionary is empty");
        }

        inputSchema = inputOperator.getOutputSchema();
        outputSchema = inputSchema;

        if (!inputSchema.containsField(SchemaConstants.PAYLOAD)) {
            outputSchema = Utils.addAttributeToSchema(outputSchema, SchemaConstants.PAYLOAD_ATTRIBUTE);
        }
        if (inputSchema.containsField(predicate.getSpanListName())) {
            throw new DataFlowException(ErrorMessages.DUPLICATE_ATTRIBUTE(predicate.getSpanListName(), inputSchema));
        } else {
            outputSchema = Utils.addAttributeToSchema(outputSchema,
                    new Attribute(predicate.getSpanListName(), AttributeType.LIST));
        }

    }

    @Override
    protected Tuple computeNextMatchingTuple() throws TextDBException {
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
    public Tuple processOneInputTuple(Tuple inputTuple) throws TextDBException {

        if (inputTuple == null) {
            return null;
        }
        if (!inputSchema.containsField(SchemaConstants.PAYLOAD)) {
            inputTuple = DataflowUtils.getSpanTuple(inputTuple.getFields(),
                    DataflowUtils.generatePayloadFromTuple(inputTuple, predicate.getAnalyzerString()), outputSchema);
        }
        if (predicate.getSpanListName() != null) {
            inputTuple = DataflowUtils.getSpanTuple(inputTuple.getFields(), new ArrayList<Span>(), outputSchema);
        }

        predicate.getDictionary().resetCursor();
        String currentDictionaryEntry;
        List<Span> matchingResults = new ArrayList<>();

        while ((currentDictionaryEntry = predicate.getDictionary().getNextEntry()) != null) {

            if(predicate.getKeywordMatchingType()== KeywordMatchingType.SUBSTRING_SCANBASED){
                DataflowUtils.appendSubstringMatchingSpans(inputTuple,predicate.getAttributeNames(), currentDictionaryEntry, matchingResults);
            }
            else if(predicate.getKeywordMatchingType() == KeywordMatchingType.PHRASE_INDEXBASED){
                DataflowUtils.appendPhraseMatchingSpans(inputTuple, predicate.getAttributeNames(), currentDictionaryEntry, predicate.getAnalyzerString(), matchingResults);
            }
            else if(predicate.getKeywordMatchingType() == KeywordMatchingType.CONJUNCTION_INDEXBASED){
                DataflowUtils.appendConjunctionMatchingSpans(inputTuple, predicate.getAttributeNames(), currentDictionaryEntry, predicate.getAnalyzerString(), matchingResults);
            }

        }

        if (matchingResults.isEmpty()) {
            return null;
        }

        ListField<Span> spanListField = inputTuple.getField(predicate.getSpanListName());
        List<Span> spanList = spanListField.getValue();
        spanList.addAll(matchingResults);

        return inputTuple;

    }



    @Override
    protected void cleanUp() throws TextDBException {

    }

    public Schema getOutputSchema() {
        return outputSchema;
    }

    public void setInputOperator(IOperator inputOperator) {
        this.inputOperator = inputOperator;
    }


    public IOperator getInputOperator() {
        return inputOperator;
    }

    public DictionaryPredicate getPredicate() {
        return this.predicate;
    }

}
