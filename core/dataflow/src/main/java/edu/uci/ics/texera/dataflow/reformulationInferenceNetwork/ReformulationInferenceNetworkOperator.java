package edu.uci.ics.texera.dataflow.reformulationInferenceNetwork;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import org.apache.commons.collections4.list.PredicatedList;

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

public class ReformulationInferenceNetworkOperator implements IOperator {
    private final ReformulationInferenceNetworkOperatorPredicate predicate;
    private IOperator inputOperator;
    private Schema outputSchema;
    
    private List<Tuple> tupleBuffer;
    private List<String> predictList;
    
    private int cursor = CLOSED;
    
    private final static String PYTHON = "python3";
    private final static String PYTHONSCRIPT = Utils.getResourcePath("RIN/rinTestTexera.py", TexeraProject.TEXERA_DATAFLOW).toString();
    private final static String BatchedFiles = Utils.getResourcePath("RIN/querySession-id-session.csv", TexeraProject.TEXERA_DATAFLOW).toString();
    private final static String resultPath = Utils.getResourcePath("RIN/querySession-result-id-class.csv", TexeraProject.TEXERA_DATAFLOW).toString();
    private final static String RinResourcePath=Utils.getResourcePath("RIN/", TexeraProject.TEXERA_DATAFLOW).toString();

    //Default nltk training model set to be "Senti.pickle"
    private String modelPath = null;
    
    public ReformulationInferenceNetworkOperator(ReformulationInferenceNetworkOperatorPredicate predicate){
        this.predicate = predicate;
        
        String modelFileName = predicate.getInputAttributeModel();
        if (modelFileName == null) {
            modelFileName = "rinModel/final/";
        }
        this.modelPath = Utils.getResourcePath("RIN/"+modelFileName, TexeraProject.TEXERA_DATAFLOW).toString();
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
        Schema.checkAttributeExists(inputSchema, predicate.getSessionInputAttrName());
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
                String[] idTextPair = new String[1];
//                idTextPair[0] = inputTuple.getField(SchemaConstants._ID).getValue().toString();
                idTextPair[0] = inputTuple.<IField>getField(predicate.getSessionInputAttrName()).getValue().toString();
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
             *      #python3 rinTestTexera picklePath inputDataPath resultPath rinResourcePath
             * */
            List<String> args = new ArrayList<String>(
                    Arrays.asList(PYTHON, PYTHONSCRIPT, modelPath, filePath, resultPath,RinResourcePath));
            ProcessBuilder processBuilder = new ProcessBuilder(args);
            
            Process p = processBuilder.start();
//            System.out.println("**************************************");
//            System.out.println("Command: "+processBuilder.command());
//            String line;
//            BufferedReader bri = new BufferedReader(new InputStreamReader(p.getInputStream()));
//            BufferedReader bre = new BufferedReader(new InputStreamReader(p.getErrorStream()));
//            System.out.println("************inputStream***************");
//            while ((line = bri.readLine()) != null) {
//            	System.out.println(line);
//            }
//            bri.close();
//            System.out.println("************outputStream***************");
//            while ((line = bre.readLine()) != null) {
//            	System.out.println(line);
//            }
//            bre.close();
            
            p.waitFor();
//            int exitVal = p.exitValue();
//            System.out.println("exitValue="+exitVal);
//            System.out.println("**************************************");
            
            //Read label result from file generated by Python.
            CSVReader csvReader = new CSVReader(new FileReader(resultPath));
            List<String[]> allRows = csvReader.readAll();
               
            predictList=new ArrayList<String>();
            //Read CSV line by line
            for(String[] row: allRows){
            	predictList.add(row[0]);
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
        String prediction=predictList.get(0);
        predictList.remove(0);
        if (tupleBuffer.isEmpty()) {
            tupleBuffer = null;
            predictList=null;
        }
        
        List<IField> outputFields = new ArrayList<>();
        outputFields.addAll(outputTuple.getFields());
        
//        String className = idClassMap.get(outputTuple.getField(SchemaConstants._ID).getValue().toString());
        outputFields.add(new StringField( prediction ));
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
        if (! inputSchema[0].containsAttribute(predicate.getSessionInputAttrName())) {
            throw new TexeraException(String.format(
                    "input attribute %s is not in the input schema %s",
                    predicate.getSessionInputAttrName(),
                    inputSchema[0].getAttributeNames()));
        }

        // check if the attribute type is valid
        AttributeType inputAttributeType =
                inputSchema[0].getAttribute(predicate.getSessionInputAttrName()).getType();
        boolean isValidType = inputAttributeType.equals(AttributeType.STRING) ||
                inputAttributeType.equals(AttributeType.TEXT);
        if (! isValidType) {
            throw new TexeraException(String.format(
                    "input attribute %s must have type String or Text, its actual type is %s",
                    predicate.getSessionInputAttrName(),
                    inputAttributeType));
        }
        return transformSchema(inputSchema[0]);
	}
}
