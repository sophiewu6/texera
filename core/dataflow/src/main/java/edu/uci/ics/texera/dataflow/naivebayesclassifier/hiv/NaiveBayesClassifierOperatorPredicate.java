package edu.uci.ics.texera.dataflow.naivebayesclassifier.hiv;

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

public class NaiveBayesClassifierOperatorPredicate extends PredicateBase{
    private final String textInputAttributeName;
    private final String labelInputAttributeName;
    private final String resultAttributeName;
    private final String inputAttributeModel;
    private final int batchSize;
    
    @JsonCreator
    public NaiveBayesClassifierOperatorPredicate(
            @JsonProperty(value = PropertyNameConstants.TEXT_ATTRIBUTE, required = true)
            String textInputAttributeName,
            @JsonProperty(value = PropertyNameConstants.LABEL_ATTRIBUTE, required = true)
            String labelInputAttributeName,
            @JsonProperty(value = PropertyNameConstants.RESULT_ATTRIBUTE_NAME, required = true)
            String resultAttributeName,
            
            @AdvancedOption
            @JsonProperty(value = PropertyNameConstants.NLP_NLTK_BATCH_SIZE, required = true,
                    defaultValue = "10")
            int batchSize,
            @JsonProperty(value = PropertyNameConstants.NLP_NLTK_MODEL, required = true,defaultValue="hiv.model")
            String inputAttributeModel) {
        if (textInputAttributeName.trim().isEmpty()) {
            throw new TexeraException("Text Input Attribute Name Cannot Be Empty");
        }
        if (labelInputAttributeName.trim().isEmpty()) {
            throw new TexeraException("Label Input Attribute Name Cannot Be Empty");
        }
        if (resultAttributeName.trim().isEmpty()) {
            throw new TexeraException("Result Attribute Name Cannot Be Empty");
        }
        this.textInputAttributeName=textInputAttributeName;
        this.labelInputAttributeName=labelInputAttributeName;
        this.resultAttributeName = resultAttributeName;
        this.batchSize = batchSize;
        this.inputAttributeModel = inputAttributeModel;
    };
    
    @JsonProperty(PropertyNameConstants.TEXT_ATTRIBUTE)
    public String getTextInputAttrName() {
    	return this.textInputAttributeName;
    }
    
    @JsonProperty(PropertyNameConstants.LABEL_ATTRIBUTE)
    public String getLabelInputAttrName() {
    	return this.labelInputAttributeName;
    }
    
    @JsonProperty(PropertyNameConstants.RESULT_ATTRIBUTE_NAME)
    public String getResultAttributeName() {
        return this.resultAttributeName;
    }
    
    @JsonProperty(PropertyNameConstants.NLP_NLTK_MODEL)
    public String getInputAttributeModel() {
        return this.inputAttributeModel;
    }
    
    @JsonProperty(PropertyNameConstants.NLP_NLTK_BATCH_SIZE)
    public int getBatchSize() {
        return this.batchSize;
    }
	@Override
	public IOperator newOperator() {
		// TODO Auto-generated method stub
		return new NaiveBayesClassifierOperator(this);
	}
	
    public static Map<String, Object> getOperatorMetadata() {
        return ImmutableMap.<String, Object>builder()
            .put(PropertyNameConstants.USER_FRIENDLY_NAME, "NaiveBayesClassifier Analysis")
            .put(PropertyNameConstants.OPERATOR_DESCRIPTION, "NaiveBayesClassifier analysis based on Python's NLTK package")
            .put(PropertyNameConstants.OPERATOR_GROUP_NAME, OperatorGroupConstants.ANALYTICS_GROUP)
            .build();
    }

}
