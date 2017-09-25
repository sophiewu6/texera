package edu.uci.ics.texera.dataflow.projection;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import edu.uci.ics.texera.api.dataflow.IOperator;
import edu.uci.ics.texera.dataflow.common.PredicateBase;
import edu.uci.ics.texera.dataflow.common.PropertyNameConstants;

public class ProjectionPredicate extends PredicateBase {
    
    private final List<String> projectionFields;
    
    @JsonCreator
    public ProjectionPredicate(
            @JsonProperty(value = PropertyNameConstants.ATTRIBUTE_NAMES, required = true)
            List<String> projectionFields) {
        this.projectionFields = projectionFields.stream().map(s -> s.toLowerCase()).collect(Collectors.toList());
    }
    
    @JsonProperty(value = PropertyNameConstants.ATTRIBUTE_NAMES)
    public List<String> getProjectionFields() {
        return new ArrayList<>(projectionFields);
    }
    
    @Override
    public IOperator newOperator() {
        return new ProjectionOperator(this);
    }
}
