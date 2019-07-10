package edu.uci.ics.texera.dataflow.nlp.keywordsextractor;

import java.util.ArrayList;
import java.util.List;
//import java.util.Properties;

import com.hankcs.hanlp.HanLP;

//import edu.stanford.nlp.ling.CoreAnnotations;
//import edu.stanford.nlp.neural.rnn.RNNCoreAnnotations;
//import edu.stanford.nlp.pipeline.Annotation;
//import edu.stanford.nlp.pipeline.StanfordCoreNLP;
//import edu.stanford.nlp.sentiment.SentimentCoreAnnotations;
//import edu.stanford.nlp.trees.Tree;
//import edu.stanford.nlp.util.CoreMap;
import edu.uci.ics.texera.api.constants.ErrorMessages;
import edu.uci.ics.texera.api.dataflow.IOperator;
import edu.uci.ics.texera.api.exception.DataflowException;
import edu.uci.ics.texera.api.exception.TexeraException;
import edu.uci.ics.texera.api.field.IField;
//import edu.uci.ics.texera.api.field.IField;
//import edu.uci.ics.texera.api.field.IntegerField;
import edu.uci.ics.texera.api.field.StringField;
import edu.uci.ics.texera.api.schema.AttributeType;
import edu.uci.ics.texera.api.schema.Schema;
import edu.uci.ics.texera.api.tuple.Tuple;

//import static edu.uci.ics.texera.api.constants.DataConstants.TexeraProject.TEXERA_DATAFLOW;

/**
 * This Operator performs keywords extractor analysis using HanLP's keywords analysis module.
 * 
 * The result will be put into an attribute with resultAttributeName specified in predicate, and type string.
 * 
 * @author Zuozhi Wang
 *
 */
public class KeywordsExtractorOperator implements IOperator {
    
    private final KeywordsExtractorPredicate predicate;
    private IOperator inputOperator;
    private Schema outputSchema;
    private int cursor = CLOSED;
    
//    StanfordCoreNLP sentimentPipeline;
    
    public KeywordsExtractorOperator(KeywordsExtractorPredicate predicate) {
        this.predicate = predicate;
    }
    
    public void setInputOperator(IOperator operator) {
        if (cursor != CLOSED) {  
            throw new TexeraException("Cannot link this operator to other operator after the operator is opened");
        }
        this.inputOperator = operator;
    }
    
    /*
     * adds a new field to the schema, with name resultAttributeName and type Integer
     */
    private Schema transformSchema(Schema inputSchema) {
        Schema.checkAttributeExists(inputSchema, predicate.getInputAttributeName());
        Schema.checkAttributeNotExists(inputSchema, predicate.getResultAttributeName());
        return new Schema.Builder().add(inputSchema).add(predicate.getResultAttributeName(), AttributeType.STRING).build();
    }

    @Override
    public void open() throws TexeraException {
        if (cursor != CLOSED) {
            return;
        }
        if (inputOperator == null) {
            throw new DataflowException(ErrorMessages.INPUT_OPERATOR_NOT_SPECIFIED);
        }
        inputOperator.open();
        Schema inputSchema = inputOperator.getOutputSchema();

        // generate output schema by transforming the input schema
        outputSchema = transformToOutputSchema(inputSchema);
        
        cursor = OPENED;
        
//        // setup NLP sentiment analysis pipeline
//        Properties props = new Properties();
//        props.setProperty("annotators", "tokenize, ssplit, parse, sentiment");
//        sentimentPipeline = new StanfordCoreNLP(props);
    }

    @Override
    public Tuple getNextTuple() throws TexeraException {
        if (cursor == CLOSED) {
            return null;
        }
        Tuple inputTuple = inputOperator.getNextTuple();
        if (inputTuple == null) {
            return null;
        }
        
        List<IField> outputFields = new ArrayList<>();
        outputFields.addAll(inputTuple.getFields());
        outputFields.add(new StringField(computeKeywords(inputTuple)));
        
        return new Tuple(outputSchema, outputFields);
    }
    
    
    private String computeKeywords(Tuple inputTuple) {
        String inputText = inputTuple.<IField>getField(predicate.getInputAttributeName()).getValue().toString();
        int keywordNumber=predicate.getKeywordNumber();
        List<String> keywordList = HanLP.extractKeyword(inputText, keywordNumber);
        return keywordList.toString();
//        return keywordNumber+"";

//        Annotation documentAnnotation = new Annotation(inputText);
//        sentimentPipeline.annotate(documentAnnotation);
        
//        // mainSentiment is calculated by the sentiment class of the longest sentence
//        Integer mainSentiment = 0;
//        Integer longestSentenceLength = 0;
//        for (CoreMap sentence : documentAnnotation.get(CoreAnnotations.SentencesAnnotation.class)) {
//            Tree tree = sentence.get(SentimentCoreAnnotations.SentimentAnnotatedTree.class);
//            int sentiment = RNNCoreAnnotations.getPredictedClass(tree);
//            String sentenceText = sentence.toString();
//            if (sentenceText.length() > longestSentenceLength) {
//                mainSentiment = sentiment;
//                longestSentenceLength=sentenceText.length();
//            }
//        }
//        return normalizeSentimentScore(mainSentiment);
    }
    
//    private static int normalizeSentimentScore(int nlpSentiment) {
//        if (nlpSentiment > 2) {
//            return SentimentConstants.POSITIVE;
//        } else if (nlpSentiment == 2) {
//            return SentimentConstants.NEUTRAL;
//        } else {
//            return SentimentConstants.NEGATIVE;
//        }
//    }

    @Override
    public void close() throws TexeraException {
        if (cursor == CLOSED) {
            return;
        }
        if (inputOperator != null) {
            inputOperator.close();
        }
        cursor = CLOSED;
    }

    @Override
    public Schema getOutputSchema() {
        return this.outputSchema;
    }

    public Schema transformToOutputSchema(Schema... inputSchema) {

        if (inputSchema.length != 1)
            throw new TexeraException(String.format(ErrorMessages.NUMBER_OF_ARGUMENTS_DOES_NOT_MATCH, 1, inputSchema.length));

        // check if input schema is present
        if (! inputSchema[0].containsAttribute(predicate.getInputAttributeName())) {
            throw new TexeraException(String.format(
                    "input attribute %s is not in the input schema %s",
                    predicate.getInputAttributeName(),
                    inputSchema[0].getAttributeNames()));
        }

        // check if attribute type is valid
        AttributeType inputAttributeType =
                inputSchema[0].getAttribute(predicate.getInputAttributeName()).getType();
        boolean isValidType = inputAttributeType.equals(AttributeType.STRING) ||
                inputAttributeType.equals(AttributeType.TEXT);
        if (! isValidType) {
            throw new TexeraException(String.format(
                    "input attribute %s must have type String or Text, its actual type is %s",
                    predicate.getInputAttributeName(),
                    inputAttributeType));
        }

        return transformSchema(inputSchema[0]);
    }

}
