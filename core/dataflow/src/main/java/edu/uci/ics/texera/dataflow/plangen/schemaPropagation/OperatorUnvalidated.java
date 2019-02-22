package edu.uci.ics.texera.dataflow.plangen.schemaPropagation;

import java.util.HashMap;

public class OperatorUnvalidated {

    private String operatorID;
    private String operatorType;
    private HashMap<String, Object> operatorProperties;

    public OperatorUnvalidated(String operatorID, String operatorType, HashMap<String, Object> operatorProperties) {
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

    public HashMap<String, Object> getOperatorProperties() {
        return operatorProperties;
    }

}
