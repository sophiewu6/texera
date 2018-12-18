package edu.uci.ics.texera.dataflow.topicmodel;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import edu.uci.ics.texera.api.constants.ErrorMessages;
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

public class TopicModelOperator implements IOperator {
	private final TopicModelOperatorPredicate predicate;
    private IOperator inputOperator;
    private Schema outputSchema;
    private int cursor = CLOSED;
    
    private List<Tuple> tupleBuffer;
    private List<String> labels=new ArrayList<>();
    private ArrayList<String> vocab=new ArrayList<>();
    private float alpha;
    private float beta;
    private float[][] topicWordPro;
    
    private final static int vocabSize=7000;
    private final static String vocabPath=Utils.getResourcePath("BBCTotalVocabulary.txt", TexeraProject.TEXERA_DATAFLOW).toString();
    private final static String topicModelPath=Utils.getResourcePath("topicModels", TexeraProject.TEXERA_DATAFLOW).toString();
    
    public TopicModelOperator(TopicModelOperatorPredicate predicate){
		// TODO Auto-generated constructor stub
    	this.predicate=predicate;
        alpha=(float)1.0/predicate.getTopicK();
        beta=(float)1.0/vocabSize;
    	try {
    		InputStreamReader inStrR = new InputStreamReader(new FileInputStream(vocabPath));
    		BufferedReader br = new BufferedReader(inStrR);
    		int i=0;
    		String line = br.readLine();
    		while (line != null&&i<vocabSize) {
    			line=line.toLowerCase();
    			vocab.add(line);
    			i++;
    			line = br.readLine();
    		}
    		br.close();
    		readTopicWord(topicModelPath+"/topicModelK"+predicate.getTopicK()+"Iter"+predicate.getIterNum()+"/globalTopicWord.txt");
		} catch (IOException e) {
			// TODO: handle exception
			throw new DataflowException(e.getMessage(), e);
		}
	}
    public float[][] readTopicWord(String filePath)throws IOException {
        topicWordPro=new float[predicate.getTopicK()][vocabSize];
        InputStreamReader inStrR = new InputStreamReader(new FileInputStream(filePath));
        BufferedReader br = new BufferedReader(inStrR);
        String line = br.readLine();
        String[] topicWord;
        int topicIndex;
        while (line != null) {
            topicWord=line.split(":");
            topicIndex=Integer.parseInt(topicWord[0]);
            String[] wordspro=topicWord[1].split(",");
            for(int i=0;i<wordspro.length;i++,i++)
            {
                topicWordPro[topicIndex][Integer.parseInt(wordspro[i])]=Float.parseFloat(wordspro[i+1]);
            }
            line = br.readLine();
        }
        br.close();
        return topicWordPro;
    }
    public int[] EnglishReadParseDocs(String document){
        document=document.replaceAll("[^A-Za-z]", " ").trim().replaceAll(" +", " ");
        String[] words=document.split(" ");
        ArrayList<Integer> wordsIndex=new ArrayList<Integer>();
        for(int j=0;j<words.length;j++)
        {
            if(vocab.contains(words[j])&&words[j].length()>1){
                wordsIndex.add(vocab.indexOf(words[j]));
            }
            else continue;
        }
        int[] doc=new int[wordsIndex.size()];
        for(int j=0;j<wordsIndex.size();j++)
        {
            doc[j]=wordsIndex.get(j);
        }
        return doc;
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
        Schema.checkAttributeExists(inputSchema, predicate.getInputAttributeName());
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
        int i = 0;
        while (i < predicate.getBatchSize()){
            Tuple inputTuple;
            if ((inputTuple = inputOperator.getNextTuple()) != null) {
                tupleBuffer.add(inputTuple);
                i++;
            } else {
                break;
            }
        }
        if (tupleBuffer.isEmpty()) {
            return false;
        }
        return true;
    }
    public void inferBatchDocsLDA(){
    	labels.clear();
        int M=tupleBuffer.size();
        int[][] docs=new int[M][];
        int[] docLength=new int[M];
        for(int i=0;i<tupleBuffer.size();i++) {
            docs[i]=EnglishReadParseDocs(tupleBuffer.get(i).<IField>getField(predicate.getInputAttributeName()).getValue().toString());
            docLength[i]=docs[i].length;
        }
        
        InferenceLDA inferenceLDA=new InferenceLDA(predicate.getTopicK(),predicate.getIterNum(),alpha,beta,docs,docLength,vocabSize,M,vocab,topicWordPro);
        inferenceLDA.inferInitial();
        inferenceLDA.inferGibbsSampling();
        
        labels=inferenceLDA.getTopKtopicsAndProbabilities(predicate.getTopKtopics());
    }

	@Override
	public Tuple getNextTuple() throws TexeraException {
		// TODO Auto-generated method stub
	       if (cursor == CLOSED) {
	            return null;
	        }
	        if (tupleBuffer == null){
	            if (computeTupleBuffer()) {
	            	inferBatchDocsLDA();
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
        
        String label = labels.get(0);
        labels.remove(0);
        outputFields.add(new StringField(label));
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
        if (! inputSchema[0].containsAttribute(predicate.getInputAttributeName())) {
            throw new TexeraException(String.format(
                    "input attribute %s is not in the input schema %s",
                    predicate.getInputAttributeName(),
                    inputSchema[0].getAttributeNames()));
        }

        // check if the attribute type is valid
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
