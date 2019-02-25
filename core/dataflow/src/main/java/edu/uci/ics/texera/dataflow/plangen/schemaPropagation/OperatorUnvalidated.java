package edu.uci.ics.texera.dataflow.plangen.schemaPropagation;

import java.util.Map;

public class OperatorUnvalidated {

    private String operatorID;
    private String operatorType;
    private Map<String, Object> operatorProperties;

    public OperatorUnvalidated(String operatorID, String operatorType, Map<String, Object> operatorProperties) {
        this.operatorID = operatorID;
        this.operatorType = operatorType;
        this.operatorProperties = operatorProperties;
    }

    public String getOperatorID() {
        return operatorID;
    }

    public String getOperatorType() {
        return operatorType;
    }

    public Map<String, Object> getOperatorProperties() {
        return operatorProperties;
    }

}
