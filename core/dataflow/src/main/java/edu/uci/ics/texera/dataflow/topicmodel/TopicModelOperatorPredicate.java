package edu.uci.ics.texera.dataflow.topicmodel;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;

import edu.uci.ics.texera.api.exception.TexeraException;
import edu.uci.ics.texera.dataflow.annotation.AdvancedOption;
import edu.uci.ics.texera.dataflow.common.OperatorGroupConstants;
import edu.uci.ics.texera.dataflow.common.PredicateBase;
import edu.uci.ics.texera.dataflow.common.PropertyNameConstants;

public class TopicModelOperatorPredicate extends PredicateBase {
    private final String inputAttributeName;
    private final String resultAttributeName;
    private final int batchSize;
    private final int K;
    private final int iterNum;
    private final int topKtopics;
    
    @JsonCreator
    public TopicModelOperatorPredicate(
            @JsonProperty(value = PropertyNameConstants.ATTRIBUTE_NAME, required = true)
            String inputAttributeName,
            @JsonProperty(value = PropertyNameConstants.RESULT_ATTRIBUTE_NAME, required = true)
            String resultAttributeName,
            
            @AdvancedOption
            @JsonProperty(value = PropertyNameConstants.TOPIC_MODEL_BATCH_SIZE, required = true,
                    defaultValue = "10")
            int batchSize,
            @JsonProperty(value=PropertyNameConstants.TOPIC_MODEL_K,required=true)
            int K,
            @JsonProperty(value=PropertyNameConstants.TOPIC_MODEL_iterNum,required=true)
            int iterNum,
            @AdvancedOption
            @JsonProperty(value=PropertyNameConstants.TOPIC_MODEL_TopK_TOPICS,required=true,defaultValue="10")
            int topKtopics) {
        if (inputAttributeName.trim().isEmpty()) {
            throw new TexeraException("Input Attribute Name Cannot Be Empty");
        }
        if (resultAttributeName.trim().isEmpty()) {
            throw new TexeraException("Result Attribute Name Cannot Be Empty");
        }
        this.inputAttributeName = inputAttributeName;
        this.resultAttributeName = resultAttributeName;
        this.batchSize = batchSize;
        this.K=K;
        this.iterNum=iterNum;
        this.topKtopics=topKtopics;
    };
    
    @JsonProperty(PropertyNameConstants.ATTRIBUTE_NAME)
    public String getInputAttributeName() {
        return this.inputAttributeName;
    }
    
    @JsonProperty(PropertyNameConstants.RESULT_ATTRIBUTE_NAME)
    public String getResultAttributeName() {
        return this.resultAttributeName;
    }
    
    @JsonProperty(PropertyNameConstants.TOPIC_MODEL_BATCH_SIZE)
    public int getBatchSize() {
        return this.batchSize;
    }
    
    @JsonProperty(PropertyNameConstants.TOPIC_MODEL_K)
    public int getTopicK() {
    	return this.K;
    }
    
    @JsonProperty(PropertyNameConstants.TOPIC_MODEL_iterNum)
    public int getIterNum() {
		return this.iterNum;
	}
    
    @JsonProperty(PropertyNameConstants.TOPIC_MODEL_TopK_TOPICS)
    public int getTopKtopics() {
		return this.topKtopics;
	}
    
	@Override
	public TopicModelOperator newOperator() {
		// TODO Auto-generated method stub
		return new TopicModelOperator(this);
	}
	
    public static Map<String, Object> getOperatorMetadata() {
        return ImmutableMap.<String, Object>builder()
            .put(PropertyNameConstants.USER_FRIENDLY_NAME, "Topic Model Analysis")
            .put(PropertyNameConstants.OPERATOR_DESCRIPTION, "Topic Model analysis based on LDA")
            .put(PropertyNameConstants.OPERATOR_GROUP_NAME, OperatorGroupConstants.ANALYTICS_GROUP)
            .build();
    }
}
