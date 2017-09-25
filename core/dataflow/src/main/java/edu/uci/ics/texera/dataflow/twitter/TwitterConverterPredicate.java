package edu.uci.ics.texera.dataflow.twitter;

import com.fasterxml.jackson.annotation.JsonCreator;

import edu.uci.ics.texera.dataflow.common.PredicateBase;

public class TwitterConverterPredicate extends PredicateBase {
        
    @JsonCreator
    public TwitterConverterPredicate() { }

    @Override
    public TwitterConverter newOperator() {
        return new TwitterConverter();
    }

}