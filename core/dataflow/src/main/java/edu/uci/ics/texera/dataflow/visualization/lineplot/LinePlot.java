package edu.uci.ics.texera.dataflow.visualization.lineplot;


import java.util.*;
import java.util.stream.Collectors;

import edu.uci.ics.texera.api.constants.ErrorMessages;
import edu.uci.ics.texera.api.constants.SchemaConstants;
import edu.uci.ics.texera.api.dataflow.IOperator;
import edu.uci.ics.texera.api.dataflow.ISink;
import edu.uci.ics.texera.api.exception.DataflowException;
import edu.uci.ics.texera.api.exception.TexeraException;
import edu.uci.ics.texera.api.field.IField;
import edu.uci.ics.texera.api.schema.Schema;
import edu.uci.ics.texera.api.tuple.Tuple;
import edu.uci.ics.texera.dataflow.visualization.lineplot.LinePlotPredicate;
import edu.uci.ics.texera.dataflow.utils.DataflowUtils;

public class LinePlot implements ISink {

    private final LinePlotPredicate predicate;

    private IOperator inputOperator;

    private Schema inputSchema;
    private Schema outputSchema;

    private int cursor = CLOSED;

    private String xAxis;
    private HashMap<IField, Integer> map;

    public LinePlot(LinePlotPredicate predicate) {
        this.predicate = predicate;
    }

    public void setInputOperator(IOperator inputOperator) {
        if (cursor != CLOSED) {
            throw new TexeraException(ErrorMessages.INPUT_OPERATOR_CHANGED_AFTER_OPEN);
        }
        this.inputOperator = inputOperator;
    }

    public IOperator getInputOperator() {
        return this.inputOperator;
    }

    @Override
    public Schema getOutputSchema() {
        return outputSchema;
    }

    @Override
    public void open() throws TexeraException {
        if (cursor != CLOSED) {
            return;
        }
        if (inputOperator == null) {
            throw new TexeraException(ErrorMessages.INPUT_OPERATOR_NOT_SPECIFIED);
        }
        inputOperator.open();
        inputSchema = inputOperator.getOutputSchema();
        outputSchema = new Schema.Builder(inputSchema)
                .removeIfExists(SchemaConstants.PAYLOAD).build();
        cursor = OPENED;
    }

    @Override
    public void processTuples() throws TexeraException {
        xAxis = predicate.getXAxis();
        Tuple inputTuple;
        while ((inputTuple = inputOperator.getNextTuple()) != null)
        {
            IField field = inputTuple.getField(xAxis);
            map.putIfAbsent(field, 0);
            map.put(field, map.get(field) + 1);
        }
    }

    @Override
    public Tuple getNextTuple() throws TexeraException {
        if (cursor == CLOSED) {
            return null;
        }
        if (cursor >= predicate.getLimit() + predicate.getOffset()) {
            return null;
        }
        Tuple resultTuple = null;
        while (true) {
            resultTuple = inputOperator.getNextTuple();
            if (resultTuple == null) {
                return null;
            }
            cursor++;
            if (cursor > predicate.getOffset()) {
                break;
            }
        }
        return new Tuple.Builder(resultTuple)
                .removeIfExists(SchemaConstants.PAYLOAD).build();

    }

    @Override
    public void close() throws TexeraException {
        if (cursor == CLOSED) {
        }
        if (inputOperator != null) {
            inputOperator.close();
        }
        cursor = CLOSED;
    }

    public Schema transformToOutputSchema(Schema... inputSchema) throws DataflowException {
        throw new TexeraException(ErrorMessages.INVALID_OUTPUT_SCHEMA_FOR_SINK);
    }
}
