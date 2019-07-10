package edu.uci.ics.texera.dataflow.nlp.sentiment.chinese;

import com.hankcs.hanlp.HanLP;
import com.hankcs.hanlp.classification.classifiers.NaiveBayesClassifier;
import com.hankcs.hanlp.classification.models.NaiveBayesModel;
import com.hankcs.hanlp.mining.cluster.ClusterAnalyzer;
import com.hankcs.hanlp.seg.common.Term;

import java.io.FileNotFoundException;

import edu.uci.ics.texera.api.constants.ErrorMessages;
import edu.uci.ics.texera.api.dataflow.IOperator;
import edu.uci.ics.texera.api.exception.DataflowException;
import edu.uci.ics.texera.api.exception.TexeraException;
import edu.uci.ics.texera.api.field.IField;
import edu.uci.ics.texera.api.field.IntegerField;
import edu.uci.ics.texera.api.field.StringField;
import edu.uci.ics.texera.api.schema.AttributeType;
import edu.uci.ics.texera.api.schema.Schema;
import edu.uci.ics.texera.api.tuple.Tuple;
import edu.uci.ics.texera.api.utils.Utils;
import org.apache.commons.lang3.SerializationUtils;

import java.io.BufferedReader;
import java.io.FileReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static edu.uci.ics.texera.api.constants.DataConstants.TexeraProject.TEXERA_DATAFLOW;
import static edu.uci.ics.texera.dataflow.nlp.sentiment.chinese.ChineseSentimentUtils.ensureTestData;

public class ChineseSentimentOperator implements IOperator {


    private final ChineseSentimentPredicate predicate;
    private IOperator inputOperator;
    private Schema outputSchema;
    private int cursor = CLOSED;

    private NaiveBayesClassifier classifier;
//    private HashMap<String, ArrayList<String>> dictionaryMap=new HashMap<>();
    private HashMap<String, String> dictionaryMap=new HashMap<>();


    public ChineseSentimentOperator(ChineseSentimentPredicate predicate) {
        this.predicate = predicate;

//        String applyPhase="时间,期限,形式审查,选题,论证,前期成果,积累,研究基础,研究成果,合作单位,学科代码,查重,限项,创新,指南,青基,面上,科学问题,国际合作,变更,绩效,费,立项依据,研究内容";
//        dictionaryMap.put("申请", new ArrayList<String>(Arrays.asList(applyPhase.split(","))));
//        String applyPhase1="研究基础,立项依据,选题,研究内容,科学问题,国际合作,论文质量,研究成果";
//        dictionaryMap.put("评审", new ArrayList<String>(Arrays.asList(applyPhase1.split(","))));
//        String applyPhase2="基金管理,项目管理,科研管理,中期检查,年度报告,结题,成果,论文,国际会议";
//        dictionaryMap.put("管理", new ArrayList<String>(Arrays.asList(applyPhase2.split(","))));
        String applyPhase="期限,形式审查,选题,论证,前期成果,积累,研究基础,研究成果,合作单位,学科代码,查重,限项,创新,指南,青基,面上,科学问题,国际合作,变更,绩效,费,立项依据,研究内容";
        dictionaryMap.put("申请", applyPhase);
        String applyPhase1="研究基础,立项依据,选题,研究内容,科学问题,国际合作,论文质量,研究成果";
        dictionaryMap.put("评审", applyPhase1);
        String applyPhase2="基金管理,项目管理,科研管理,中期检查,年度报告,结题,成果,论文,国际会议";
        dictionaryMap.put("管理", applyPhase2);
    }

    public void setInputOperator(IOperator operator) {
        if (cursor != CLOSED) {
            throw new TexeraException("Cannot link this operator to other operator after the operator is opened");
        }
        this.inputOperator = operator;
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

        classifier = getOrCreateModel();
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

        List<IField> outputFields = new ArrayList<>(inputTuple.getFields());
        outputFields.add(new StringField(computeSentimentScore(inputTuple)));

        return new Tuple(outputSchema, outputFields);
    }


    private String computeSentimentScore(Tuple inputTuple) {
        try {
        	String tweet=inputTuple.getField(this.predicate.getInputAttributeName()).getValue().toString();
        	String predictClass = classifier.classify(tweet);
        	List<Term> terms=HanLP.segment(tweet);
        	List<String> positiveTerms=new ArrayList<>();
        	List<String> negativeTerms=new ArrayList<>();
        	for(int i=0;i<terms.size();i++) {
        		String termPredict=classifier.classify(terms.get(i).word);
        		if(termPredict.equals("正面"))positiveTerms.add(terms.get(i).word);
        		else if(termPredict.equals("负面"))negativeTerms.add(terms.get(i).word);
        	}
//            String predictClass = classifier.classify(
//                    inputTuple.getField(this.predicate.getInputAttributeName()).getValue().toString());
            if (predictClass.equals("正面")) {
//            	HashMap<String, ArrayList<String>> positiveMap=new HashMap<>();
//            	for(Map.Entry<String, ArrayList<String>> entity: dictionaryMap.entrySet()) {
//            		String dictionary=entity.getKey();
//            		ArrayList<String> list=entity.getValue();
//            		ArrayList<String> wordList=new ArrayList<>();
//            		for(int j=0;j<list.size();j++) {
//            			
//            		}
//            		list.retainAll(positiveTerms);
//            		if(list.size()>0)positiveMap.put(dictionary, list);
//            	}
//                return "1,"+positiveMap.toString();
            	HashMap<String, ArrayList<String>> positiveMap=new HashMap<>();
            	for(Map.Entry<String, String> entity: dictionaryMap.entrySet()) {
            		String dictionary=entity.getKey();
            		String keywords=entity.getValue();
            		ArrayList<String> wordList=new ArrayList<>();
            		for(int j=0;j<positiveTerms.size();j++) {
            			String term=positiveTerms.get(j);
            			if(keywords.contains(term)&&!wordList.contains(term))wordList.add(term);
            		}
            		if(wordList.size()>0)positiveMap.put(dictionary, wordList);
            	}
            	return "1,"+positiveMap.toString();
            } else if (predictClass.equals("负面")) {
//            	HashMap<String, ArrayList<String>> negativeMap=new HashMap<>();
//            	for(Map.Entry<String, ArrayList<String>> entity: dictionaryMap.entrySet()) {
//            		String dictionary=entity.getKey();
//            		ArrayList<String> list=entity.getValue();
//            		list.retainAll(negativeTerms);
//            		if(list.size()>0)negativeMap.put(dictionary, list);
//            	}
//                return "-1,"+negativeMap.toString();
            	HashMap<String, ArrayList<String>> negativeMap=new HashMap<>();
            	for(Map.Entry<String, String> entity: dictionaryMap.entrySet()) {
            		String dictionary=entity.getKey();
            		String keywords=entity.getValue();
            		ArrayList<String> wordList=new ArrayList<>();
            		for(int j=0;j<negativeTerms.size();j++) {
            			String term=negativeTerms.get(j);
            			if(keywords.contains(term)&&!wordList.contains(term))wordList.add(term);
            		}
            		if(wordList.size()>0)negativeMap.put(dictionary, wordList);
            	}
            	return "-1,"+negativeMap.toString();
            } else {
                throw new DataflowException("unknown chinese sentiment class" + predictClass);
            }
        } catch (Exception e) {
            throw new DataflowException(e);
        }
    }

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


    /*
     * adds a new field to the schema, with name resultAttributeName and type Integer
     */
    private Schema transformSchema(Schema inputSchema) {
        Schema.checkAttributeExists(inputSchema, predicate.getInputAttributeName());
        Schema.checkAttributeNotExists(inputSchema, predicate.getResultAttributeName());
        return new Schema.Builder().add(inputSchema).add(predicate.getResultAttributeName(), AttributeType.STRING).build();
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

    @Override
    public Schema getOutputSchema() {
        return this.outputSchema;
    }


    private static NaiveBayesClassifier getOrCreateModel() {

        try {
            Path modelPath = Utils.getResourcePath("cn-sentiment.model", TEXERA_DATAFLOW);

            if (! Files.exists(modelPath)) {
                String CORPUS_FOLDER = ensureTestData("ChnSentiCorp情感分析酒店评论",
                        "http://file.hankcs.com/corpus/ChnSentiCorp.zip");
                NaiveBayesClassifier classifier = new NaiveBayesClassifier(); // create the classifier
                classifier.train(CORPUS_FOLDER);

                byte[] bytes = SerializationUtils.serialize(classifier.getModel());
                Files.createFile(modelPath);
                Files.write(modelPath, bytes);

                return classifier;
            } else {
                Object modelObject = SerializationUtils.deserialize(Files.readAllBytes(modelPath));
                return new NaiveBayesClassifier((NaiveBayesModel) modelObject);
            }
        } catch (Exception e) {
            throw new DataflowException(e);
        }

    }


    public static void test() throws Exception {

        String CORPUS_FOLDER = ensureTestData("ChnSentiCorp情感分析酒店评论",
                "http://file.hankcs.com/corpus/ChnSentiCorp.zip");
        NaiveBayesClassifier classifier = new NaiveBayesClassifier(); // create the classifier
        classifier.train(CORPUS_FOLDER);

        String text1 = "前台客房服务态度非常好！早餐很丰富，房价很干净。再接再厉！";
        String text2 = "结果大失所望，灯光昏暗，空间极其狭小，床垫质量恶劣，房间还伴着一股霉味。";
        String text3 = "可利用文本分类实现情感分析，效果还行";

        System.out.printf("《%s》 情感极性是 【%s】\n", text1, classifier.classify(text1));
        System.out.printf("《%s》 情感极性是 【%s】\n", text2, classifier.classify(text2));
        System.out.printf("《%s》 情感极性是 【%s】\n", text3, classifier.classify(text3));

        System.out.println("首次编译运行时，HanLP会自动构建词典缓存，请稍候……");
        HanLP.Config.enableDebug();         // 为了避免你等得无聊，开启调试模式说点什么:-)
        System.out.println(HanLP.segment("你好，欢迎使用HanLP汉语处理包！接下来请从其他Demo中体验HanLP丰富的功能~"));

    }


}
