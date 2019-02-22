package edu.uci.ics.texera.dataflow.plangen.schemaPropagation;

import edu.uci.ics.texera.dataflow.plangen.OperatorLink;

import java.util.List;

public class WorkflowUnvalidated {

    private List<OperatorUnvalidated> operators;
    private List<OperatorLink> links;

    public WorkflowUnvalidated(List<OperatorUnvalidated> operators, List<OperatorLink> links) {
        this.operators = operators;
        this.links = links;
    }

    public List<OperatorUnvalidated> getOperators() {
        return operators;
    }

    public List<OperatorLink> getLinks() {
        return links;
    }

}
