package edu.uci.ics.texera.dataflow.wordcount;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import edu.uci.ics.texera.api.dataflow.IOperator;
import edu.uci.ics.texera.dataflow.common.PredicateBase;
import edu.uci.ics.texera.dataflow.common.PropertyNameConstants;

/**
 * @author Qinhua Huang
 */

public class WordCountOperatorPredicate extends PredicateBase {
        
    private final String attribute;
    private final String analyzer;
    
    @JsonCreator
    public WordCountOperatorPredicate(
            @JsonProperty(value = PropertyNameConstants.ATTRIBUTE_NAME, required = true)
            String attribute,
            @JsonProperty(value = PropertyNameConstants.LUCENE_ANALYZER_STRING, required = true)
            String analyzer) {
        
        this.attribute = attribute;
        this.analyzer = analyzer;
    }
    
    @JsonProperty(PropertyNameConstants.ATTRIBUTE_NAME)
    public String getAttribute() {
        return this.attribute;
    }
    
    @JsonProperty(PropertyNameConstants.LUCENE_ANALYZER_STRING)
    public String getLuceneAnalyzerString() {
        return this.analyzer;
    }

    @Override
    public IOperator newOperator() {
        return new WordCountOperator(this);
    }
    
}
