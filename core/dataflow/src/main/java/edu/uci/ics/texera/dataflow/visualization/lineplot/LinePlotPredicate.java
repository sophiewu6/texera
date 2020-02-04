package edu.uci.ics.texera.dataflow.visualization.lineplot;

import java.util.Map;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;

import edu.uci.ics.texera.dataflow.common.OperatorGroupConstants;
import edu.uci.ics.texera.dataflow.common.PredicateBase;
import edu.uci.ics.texera.dataflow.common.PropertyNameConstants;

public class LinePlotPredicate extends PredicateBase{
    public static Map<String, Object> getOperatorMetadata() {
        return ImmutableMap.<String, Object>builder()
            .put(PropertyNameConstants.USER_FRIENDLY_NAME, "Line Plot")
            .put(PropertyNameConstants.OPERATOR_DESCRIPTION, "View the results in a line plot")
            .put(PropertyNameConstants.OPERATOR_GROUP_NAME, OperatorGroupConstants.DATABASE_GROUP)
            .build();
    }
}