package edu.uci.ics.texera.dataflow.projection;

import java.util.List;
import java.util.stream.Collectors;

import edu.uci.ics.texera.api.constants.ErrorMessages;
import edu.uci.ics.texera.api.exception.DataflowException;
import edu.uci.ics.texera.api.exception.TexeraException;
import edu.uci.ics.texera.api.field.IField;
import edu.uci.ics.texera.api.schema.Attribute;
import edu.uci.ics.texera.api.schema.Schema;
import edu.uci.ics.texera.api.tuple.Tuple;
import edu.uci.ics.texera.dataflow.common.AbstractSingleInputOperator;
import edu.uci.ics.texera.dataflow.lineage.DatabaseConnector;

public class ProjectionOperator extends AbstractSingleInputOperator {
	private String operatorName;
    private int outputID=1;
    
    ProjectionPredicate predicate;
    
    Schema inputSchema;
    
    public ProjectionOperator(ProjectionPredicate predicate) {
        this.predicate = predicate;
    }

    @Override
    protected void setUp() throws TexeraException {
        inputSchema = inputOperator.getOutputSchema();
        outputSchema = transformToOutputSchema(inputSchema);
        outputSchema=DatabaseConnector.addLineageIDAttribute(outputSchema);
    }

    @Override
    protected Tuple computeNextMatchingTuple() throws TexeraException {
        Tuple inputTuple = inputOperator.getNextTuple();
        if (inputTuple == null) {
            return null;
        }

        return processOneInputTuple(inputTuple);
    }

    @Override
    public Tuple processOneInputTuple(Tuple inputTuple) throws TexeraException {
        IField[] outputFields =
                outputSchema.getAttributes()
                        .stream()
                        .map(attr -> inputTuple.getField(attr.getName()))
                        .toArray(IField[]::new);

        if(outputID==1) {
            String className=this.getClass().getName();
            this.operatorName=className.substring(className.lastIndexOf(".")+1,className.length());
            Schema schema=getOutputSchema();
            DatabaseConnector.createResultTable(this.operatorName+"Result", schema);
            DatabaseConnector.createLineageTable(operatorName+"Lineage");
            DatabaseConnector.deleteTupleFromResultCatalogTable(operatorName);
            DatabaseConnector.deleteTupleFromLineageCatalogTable(operatorName);
            DatabaseConnector.insertTupleIntoResultCatalogTable(operatorName);
            DatabaseConnector.insertTupleIntoLineageCatalogTable(operatorName);
        }
        int inputID=DatabaseConnector.getInputTupleLineageID(inputTuple);
        Tuple outputTuple=new Tuple(outputSchema, outputFields);
        Tuple newOutputTuple=DatabaseConnector.addTupleLineageIDToOutput(outputTuple, outputID);
        String tupleContent=newOutputTuple.toString().replaceAll("'", "''");
        DatabaseConnector.insertTupleToResultTable(operatorName+"Result", tupleContent);
        DatabaseConnector.insertTupleIntoLineageTable(operatorName+"Lineage", inputID, outputID);
        outputID++;
        return outputTuple;
    }

    @Override
    protected void cleanUp() throws DataflowException {        
    }

    public ProjectionPredicate getPredicate() {
        return predicate;
    }

    public Schema transformToOutputSchema(Schema... inputSchema) {
        if (inputSchema.length != 1)
            throw new TexeraException(String.format(ErrorMessages.NUMBER_OF_ARGUMENTS_DOES_NOT_MATCH, 1, inputSchema.length));

        List<Attribute> outputAttributes =
                inputSchema[0].getAttributes()
                        .stream()
                        .filter(attr -> predicate.getProjectionFields().contains(attr.getName().toLowerCase()))
                        .collect(Collectors.toList());

        if (outputAttributes.size() != predicate.getProjectionFields().size()) {
            throw new DataflowException("input schema doesn't contain one of the attributes to be projected");
        }
        return new Schema(outputAttributes.stream().toArray(Attribute[]::new));
    }
}
