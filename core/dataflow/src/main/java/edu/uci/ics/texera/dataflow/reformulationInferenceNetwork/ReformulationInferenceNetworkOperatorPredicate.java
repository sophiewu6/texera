package edu.uci.ics.texera.dataflow.reformulationInferenceNetwork;

import java.util.Map;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;

import edu.uci.ics.texera.api.dataflow.IOperator;
import edu.uci.ics.texera.api.exception.TexeraException;
import edu.uci.ics.texera.dataflow.annotation.AdvancedOption;
import edu.uci.ics.texera.dataflow.common.OperatorGroupConstants;
import edu.uci.ics.texera.dataflow.common.PredicateBase;
import edu.uci.ics.texera.dataflow.common.PropertyNameConstants;

public class ReformulationInferenceNetworkOperatorPredicate extends PredicateBase{
	private final String sessionInputAttributeName;
	private final String resultAttributeName;
	private final String inputAttributeModel;
	private final int batchSize;
	
    @JsonCreator
    public ReformulationInferenceNetworkOperatorPredicate(
            @JsonProperty(value = PropertyNameConstants.SESSION_ATTRIBUTE, required = true)
            String sessionInputAttributeName,
//            @JsonProperty(value = PropertyNameConstants.LABEL_ATTRIBUTE, required = true)
//            String labelInputAttributeName,
            @JsonProperty(value = PropertyNameConstants.RESULT_ATTRIBUTE_NAME, required = true)
            String resultAttributeName,
            
            @AdvancedOption
            @JsonProperty(value = PropertyNameConstants.DEEPLEARNING_BATCH_SIZE, required = true,
                    defaultValue = "100")
            int batchSize,
            @JsonProperty(value = PropertyNameConstants.DEEPLEARNING_MODEL, required = true,defaultValue="rinModel/final/")
            String inputAttributeModel) {
        if (sessionInputAttributeName.trim().isEmpty()) {
            throw new TexeraException("Session Text Input Attribute Name Cannot Be Empty");
        }
//        if (labelInputAttributeName.trim().isEmpty()) {
//            throw new TexeraException("Label Input Attribute Name Cannot Be Empty");
//        }
        if (resultAttributeName.trim().isEmpty()) {
            throw new TexeraException("Result Attribute Name Cannot Be Empty");
        }
        this.sessionInputAttributeName=sessionInputAttributeName;
//        this.labelInputAttributeName=labelInputAttributeName;
        this.resultAttributeName = resultAttributeName;
        this.batchSize = batchSize;
        this.inputAttributeModel = inputAttributeModel;
    };
    
    @JsonProperty(PropertyNameConstants.SESSION_ATTRIBUTE)
    public String getSessionInputAttrName() {
    	return this.sessionInputAttributeName;
    }
    
    @JsonProperty(PropertyNameConstants.RESULT_ATTRIBUTE_NAME)
    public String getResultAttributeName() {
        return this.resultAttributeName;
    }
    
    @JsonProperty(PropertyNameConstants.DEEPLEARNING_MODEL)
    public String getInputAttributeModel() {
        return this.inputAttributeModel;
    }
    
    @JsonProperty(PropertyNameConstants.DEEPLEARNING_BATCH_SIZE)
    public int getBatchSize() {
        return this.batchSize;
    }
    
	@Override
	public IOperator newOperator() {
		// TODO Auto-generated method stub
		return new ReformulationInferenceNetworkOperator(this);
	}
    public static Map<String, Object> getOperatorMetadata() {
        return ImmutableMap.<String, Object>builder()
            .put(PropertyNameConstants.USER_FRIENDLY_NAME, "Reformulation Inference Network Analysis")
            .put(PropertyNameConstants.OPERATOR_DESCRIPTION, "Reformulation Inference Network analysis based on paper: 'RIN: Reformulation Inference Network for Context-Aware Query Suggestion'")
            .put(PropertyNameConstants.OPERATOR_GROUP_NAME, OperatorGroupConstants.ANALYTICS_GROUP)
            .build();
    }

}
