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

    private final String xAxis;

    @JsonCreator
    public LinePlotSinkPredicate (
            @JsonProperty(value = PropertyNameConstants.LINEPLOT_XAXIS, required = true)
                    String xAxis) {

        this.xAxis = xAxis;

    }

    @JsonProperty(value = PropertyNameConstants.LINEPLOT_XAXIS)
    public String getXAxis() { return xAxis; }

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