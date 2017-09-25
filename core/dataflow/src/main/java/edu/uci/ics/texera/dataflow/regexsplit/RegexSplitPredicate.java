package edu.uci.ics.texera.dataflow.regexsplit;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonValue;

import edu.uci.ics.texera.api.dataflow.IOperator;
import edu.uci.ics.texera.dataflow.common.PredicateBase;
import edu.uci.ics.texera.dataflow.common.PropertyNameConstants;

/**
 * @author Qinhua Huang
 *
 */
public class RegexSplitPredicate extends PredicateBase {
    
    public enum SplitType {
        GROUP_LEFT("left"), // the regex is grouped with the text on its left

        GROUP_RIGHT("right"), // the regex is grouped with the text on its right

        STANDALONE("standalone"); // the regex becomes a standalone tuple
        
        private final String name;
        
        private SplitType(String name) {
            this.name = name;
        }
        
        // use the name string instead of enum string in JSON
        @JsonValue
        public String getName() {
            return this.name;
        }
    }
    
    private final String splitRegex;
    private final String inputAttributeName;
    private final String resultAttributeName;

    private final SplitType splitType;
    private final RegexOutputType outputType;
    
    /**
     * Construct a RegexSplitPredicate.
     * 
     * @param regex, the regex query
     * @param attributeToSplit, the attribute name to perform split operation on
     * @param splitType, a type to indicate where the regex pattern merge into. 
     */
    @JsonCreator
    public RegexSplitPredicate(@JsonProperty(value = PropertyNameConstants.REGEX_OUTPUT_TYPE, required = true)
            RegexOutputType outputType,
            @JsonProperty(value=PropertyNameConstants.SPLIT_REGEX, required=true)
            String splitRegex,
            @JsonProperty(value=PropertyNameConstants.ATTRIBUTE_NAME, required=true)
            String splitAttribute,
            @JsonProperty(value=PropertyNameConstants.SPLIT_TYPE, required=true)
            SplitType splitType,
            @JsonProperty(value = PropertyNameConstants.RESULT_ATTRIBUTE_NAME, required = true)
            String resultAttributeName) {
        this.outputType = outputType;
        this.splitRegex = splitRegex;
        this.inputAttributeName = splitAttribute;
        
        this.splitType = splitType;
        this.resultAttributeName = resultAttributeName;
    }
    @JsonProperty(PropertyNameConstants.REGEX_OUTPUT_TYPE)
    public RegexOutputType getOutputType() {
        return this.outputType;
    }
    
    @JsonProperty(PropertyNameConstants.SPLIT_REGEX)
    public String getRegex() {
        return splitRegex;
    }
    
    @JsonProperty(PropertyNameConstants.ATTRIBUTE_NAME)
    public String getInputAttributeName() {
        return inputAttributeName;
    }
    
    @JsonProperty(PropertyNameConstants.SPLIT_TYPE)
    public SplitType getSplitType() {
        return splitType;
    }
    
    @JsonProperty(PropertyNameConstants.RESULT_ATTRIBUTE_NAME)
    public String getResultAttributeName() {
        return this.resultAttributeName;
    }
    
    @Override
    public IOperator newOperator() {
        return new RegexSplitOperator(this);
    }
    
}
