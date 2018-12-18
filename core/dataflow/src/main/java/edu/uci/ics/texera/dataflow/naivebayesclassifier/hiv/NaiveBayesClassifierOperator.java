package edu.uci.ics.texera.dataflow.naivebayesclassifier.hiv;

import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import au.com.bytecode.opencsv.CSVReader;
import au.com.bytecode.opencsv.CSVWriter;
import edu.uci.ics.texera.api.constants.ErrorMessages;
import edu.uci.ics.texera.api.constants.SchemaConstants;
import edu.uci.ics.texera.api.constants.DataConstants.TexeraProject;
import edu.uci.ics.texera.api.dataflow.IOperator;
import edu.uci.ics.texera.api.exception.DataflowException;
import edu.uci.ics.texera.api.exception.TexeraException;
import edu.uci.ics.texera.api.field.IField;
import edu.uci.ics.texera.api.field.StringField;
import edu.uci.ics.texera.api.schema.AttributeType;
import edu.uci.ics.texera.api.schema.Schema;
import edu.uci.ics.texera.api.tuple.Tuple;
import edu.uci.ics.texera.api.utils.Utils;

public class NaiveBayesClassifierOperator implements IOperator {
    private final NaiveBayesClassifierOperatorPredicate predicate;
    private IOperator inputOperator;
    private Schema outputSchema;
    
    private List<Tuple> tupleBuffer;
    HashMap<String, String> idClassMap;
    
    private int cursor = CLOSED;
    
    private final static String PYTHON = "python3";
    private final static String PYTHONSCRIPT = Utils.getResourcePath("TwitterHivClassifier.py", TexeraProject.TEXERA_DATAFLOW).toString();
    private final static String BatchedFiles = Utils.getResourcePath("hiv-id-text.csv", TexeraProject.TEXERA_DATAFLOW).toString();
    private final static String resultPath = Utils.getResourcePath("hiv-result-id-class.csv", TexeraProject.TEXERA_DATAFLOW).toString();
    
    //Default nltk training model set to be "Senti.pickle"
    private String PicklePath = null;
    
    public NaiveBayesClassifierOperator(NaiveBayesClassifierOperatorPredicate predicate){
        this.predicate = predicate;
        
        String modelFileName = predicate.getInputAttributeModel();
        if (modelFileName == null) {
            modelFileName = "hiv.model";
        }
        this.PicklePath = Utils.getResourcePath(modelFileName, TexeraProject.TEXERA_DATAFLOW).toString();
        
    }
    public void setInputOperator(IOperator operator) {
        if (cursor != CLOSED) {
            throw new TexeraException("Cannot link this operator to another operator after the operator is opened");
        }
        this.inputOperator = operator;
    }
    /*
     * add a new field to the schema, with name resultAttributeName and type String
     */
    private Schema transformSchema(Schema inputSchema){
        Schema.checkAttributeExists(inputSchema, predicate.getTextInputAttrName());
//        Schema.checkAttributeExists(inputSchema, predicate.getLabelInputAttrName());
        Schema.checkAttributeNotExists(inputSchema, predicate.getResultAttributeName());
        return new Schema.Builder().add(inputSchema).add(predicate.getResultAttributeName(), AttributeType.STRING).build();
    }

	@Override
	public void open() throws TexeraException {
		// TODO Auto-generated method stub
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
	}
	
    private boolean computeTupleBuffer() {
        tupleBuffer = new ArrayList<Tuple>();
        //write [ID,text] to a CSV file.
        List<String[]> csvData = new ArrayList<>();
        int i = 0;
        while (i < predicate.getBatchSize()){
            Tuple inputTuple;
            if ((inputTuple = inputOperator.getNextTuple()) != null) {
                tupleBuffer.add(inputTuple);
                String[] idTextPair = new String[2];
                idTextPair[0] = inputTuple.getField(SchemaConstants._ID).getValue().toString();
                idTextPair[1] = inputTuple.<IField>getField(predicate.getTextInputAttrName()).getValue().toString();
//                idTextPair[2] = inputTuple.<IField>getField(predicate.getLabelInputAttrName()).getValue().toString();
                csvData.add(idTextPair);
                i++;
            } else {
                break;
            }
        }
        if (tupleBuffer.isEmpty()) {
            return false;
        }
        try {
        		if (Files.notExists(Paths.get(BatchedFiles))) {
        			Files.createFile(Paths.get(BatchedFiles));
        		}
            CSVWriter writer = new CSVWriter(new FileWriter(BatchedFiles));
            writer.writeAll(csvData);
            writer.close();
        } catch (IOException e) {
            throw new DataflowException(e.getMessage(), e);
        }
        return true;
    }
    // Process the data file using NLTK
    private String computeClassLabel(String filePath) {
	    try{
            /*
             *  In order to use the NLTK package to do classification, we start a
             *  new process to run the package, and wait for the result of running
             *  the process as the class label of this text field.
             *  Python call format:
             *      #python3 nltk_sentiment_classify picklePath dataPath resultPath
             * */
            List<String> args = new ArrayList<String>(
                    Arrays.asList(PYTHON, PYTHONSCRIPT, PicklePath, filePath, resultPath));
            ProcessBuilder processBuilder = new ProcessBuilder(args);
            
            Process p = processBuilder.start();
            p.waitFor();
            
            //Read label result from file generated by Python.
            CSVReader csvReader = new CSVReader(new FileReader(resultPath));
            List<String[]> allRows = csvReader.readAll();
               
            idClassMap = new HashMap<String, String>();
            //Read CSV line by line
            for(String[] row : allRows){
            	idClassMap.put(row[0], row[1]);
            }
            csvReader.close();
        }catch(Exception e){
            throw new DataflowException(e.getMessage(), e);
        }
        return null;
    }

	@Override
	public Tuple getNextTuple() throws TexeraException {
		// TODO Auto-generated method stub
        if (cursor == CLOSED) {
            return null;
        }
        if (tupleBuffer == null){
            if (computeTupleBuffer()) {
                computeClassLabel(BatchedFiles);
            } else {
                return null;
            }
        }
        return popupOneTuple();
	}
    private Tuple popupOneTuple() {
        Tuple outputTuple = tupleBuffer.get(0);
        tupleBuffer.remove(0);
        if (tupleBuffer.isEmpty()) {
            tupleBuffer = null;
        }
        
        List<IField> outputFields = new ArrayList<>();
        outputFields.addAll(outputTuple.getFields());
        
        String className = idClassMap.get(outputTuple.getField(SchemaConstants._ID).getValue().toString());
        outputFields.add(new StringField( className ));
        return new Tuple(outputSchema, outputFields);
    }

	@Override
	public void close() throws TexeraException {
		// TODO Auto-generated method stub
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
		// TODO Auto-generated method stub
		return this.outputSchema;
	}

	@Override
	public Schema transformToOutputSchema(Schema... inputSchema) {
		// TODO Auto-generated method stub
        if (inputSchema.length != 1)
            throw new TexeraException(String.format(ErrorMessages.NUMBER_OF_ARGUMENTS_DOES_NOT_MATCH, 1, inputSchema.length));

        // check if the input schema is presented
        if (! inputSchema[0].containsAttribute(predicate.getTextInputAttrName())) {
            throw new TexeraException(String.format(
                    "input attribute %s is not in the input schema %s",
                    predicate.getTextInputAttrName(),
                    inputSchema[0].getAttributeNames()));
        }
//        if (! inputSchema[0].containsAttribute(predicate.getLabelInputAttrName())) {
//            throw new TexeraException(String.format(
//                    "input attribute %s is not in the input schema %s",
//                    predicate.getLabelInputAttrName(),
//                    inputSchema[0].getAttributeNames()));
//        }

        // check if the attribute type is valid
        AttributeType inputAttributeType =
                inputSchema[0].getAttribute(predicate.getTextInputAttrName()).getType();
        boolean isValidType = inputAttributeType.equals(AttributeType.STRING) ||
                inputAttributeType.equals(AttributeType.TEXT);
        if (! isValidType) {
            throw new TexeraException(String.format(
                    "input attribute %s must have type String or Text, its actual type is %s",
                    predicate.getTextInputAttrName(),
                    inputAttributeType));
        }
//        inputAttributeType =
//                inputSchema[0].getAttribute(predicate.getLabelInputAttrName()).getType();
//        isValidType = inputAttributeType.equals(AttributeType.STRING) ||
//                inputAttributeType.equals(AttributeType.TEXT);
//        if (! isValidType) {
//            throw new TexeraException(String.format(
//                    "input attribute %s must have type String or Text, its actual type is %s",
//                    predicate.getLabelInputAttrName(),
//                    inputAttributeType));
//        }

        return transformSchema(inputSchema[0]);
	}

}
