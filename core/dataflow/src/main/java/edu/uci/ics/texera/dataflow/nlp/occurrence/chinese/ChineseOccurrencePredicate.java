package edu.uci.ics.texera.dataflow.nlp.occurrence.chinese;

import java.util.Map;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;

import edu.uci.ics.texera.api.exception.TexeraException;
import edu.uci.ics.texera.dataflow.common.OperatorGroupConstants;
import edu.uci.ics.texera.dataflow.common.PredicateBase;
import edu.uci.ics.texera.dataflow.common.PropertyNameConstants;

public class ChineseOccurrencePredicate extends PredicateBase {
    
    private final String inputAttributeName;
    private final String resultAttributeName;
    private final int orderNumber;
    
    @JsonCreator
    public ChineseOccurrencePredicate(
            @JsonProperty(value = PropertyNameConstants.ATTRIBUTE_NAME, required = true)
            String inputAttributeName,
            @JsonProperty(value = PropertyNameConstants.RESULT_ATTRIBUTE_NAME, required = true)
            String resultAttributeName,
            @JsonProperty(value = PropertyNameConstants.ORDER_NUMBER, required = true)
            String orderNumber
            ) {
        if (inputAttributeName.trim().isEmpty()) {
            throw new TexeraException("Input Attribute Name Cannot Be Empty");
        }
        if (resultAttributeName.trim().isEmpty()) {
            throw new TexeraException("Result Attribute Name Cannot Be Empty");
        }
        this.inputAttributeName = inputAttributeName;
        this.resultAttributeName = resultAttributeName;
        this.orderNumber=Integer.parseInt(orderNumber);       //整型转字符串
    }
    
    @JsonProperty(PropertyNameConstants.ATTRIBUTE_NAME)
    public String getInputAttributeName() {
        return this.inputAttributeName;
    }
    
    @JsonProperty(PropertyNameConstants.RESULT_ATTRIBUTE_NAME)
    public String getResultAttributeName() {
        return this.resultAttributeName;
    }
    
    @JsonProperty(PropertyNameConstants.ORDER_NUMBER)
    public int getorderNumber() {
        return this.orderNumber;
    }
    
    @Override
    public ChineseOccurrenceOperator newOperator() {
        return new ChineseOccurrenceOperator(this);
    }
    
    public static Map<String, Object> getOperatorMetadata() {
        return ImmutableMap.<String, Object>builder()
            .put(PropertyNameConstants.USER_FRIENDLY_NAME, "Chinese Occurrence Analysis")
            .put(PropertyNameConstants.OPERATOR_DESCRIPTION, "Chinese Occurrence analysis based on HanLP package")
            .put(PropertyNameConstants.OPERATOR_GROUP_NAME, OperatorGroupConstants.ANALYTICS_GROUP)
            .build();
    }

}
