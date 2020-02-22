package edu.uci.ics.texera.dataflow.visualization.lineplot;

import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;

import edu.uci.ics.texera.dataflow.common.OperatorGroupConstants;
import edu.uci.ics.texera.dataflow.common.PredicateBase;
import edu.uci.ics.texera.dataflow.common.PropertyNameConstants;

public class LinePlotSinkPredicate extends PredicateBase {

    private final String attributeName;

    @JsonCreator
    public LinePlotSinkPredicate (
            @JsonProperty(value = PropertyNameConstants.ATTRIBUTE_NAME, required = true)
                    String attributeName) {

        this.attributeName = attributeName;

    }

    @JsonProperty(value = PropertyNameConstants.ATTRIBUTE_NAME)
    public String getAttributeName() { return attributeName; }

    @Override
    public LinePlotSink newOperator() {
        return new LinePlotSink(this);
    }

    public static Map<String, Object> getOperatorMetadata() {
        return ImmutableMap.<String, Object>builder()
            .put(PropertyNameConstants.USER_FRIENDLY_NAME, "Line Plot")
            .put(PropertyNameConstants.OPERATOR_DESCRIPTION, "View the results in a line plot")
            .put(PropertyNameConstants.OPERATOR_GROUP_NAME, OperatorGroupConstants.VISUALIZATION_GROUP)
            .build();
    }
}