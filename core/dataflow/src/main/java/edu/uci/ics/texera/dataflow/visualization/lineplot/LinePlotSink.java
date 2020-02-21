package edu.uci.ics.texera.dataflow.visualization.lineplot;


import java.util.*;
import java.util.stream.Collectors;

import com.google.protobuf.MapEntry;
import edu.uci.ics.texera.api.constants.ErrorMessages;
import edu.uci.ics.texera.api.constants.SchemaConstants;
import edu.uci.ics.texera.api.dataflow.IOperator;
import edu.uci.ics.texera.api.dataflow.ISink;
import edu.uci.ics.texera.api.exception.DataflowException;
import edu.uci.ics.texera.api.exception.TexeraException;
import edu.uci.ics.texera.api.field.IField;
import edu.uci.ics.texera.api.field.IntegerField;
import edu.uci.ics.texera.api.schema.AttributeType;
import edu.uci.ics.texera.api.schema.Schema;
import edu.uci.ics.texera.api.tuple.Tuple;
import edu.uci.ics.texera.dataflow.visualization.lineplot.LinePlotSinkPredicate;
import edu.uci.ics.texera.dataflow.utils.DataflowUtils;

public class LinePlotSink implements ISink {

    private final LinePlotSinkPredicate predicate;

    private IOperator inputOperator;

    private Schema inputSchema;
    private Schema outputSchema;

    private int cursor = CLOSED;

    private String xAxis;
    private HashMap<String, IField> result = new HashMap<>();

    public LinePlotSink(LinePlotSinkPredicate predicate) {
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
            String field = inputTuple.getField(xAxis).getValue().toString();
            result.putIfAbsent(field, new IntegerField(0));
            result.put(field, new IntegerField(((IntegerField)result.get(field)).getValue() + 1));
        }
        for (Map.Entry<String, IField> entry : result.entrySet())
        {
            System.out.println("Key = " + entry.getKey() + ", Value = " + entry.getValue());
        }
    }

    @Override
    public Tuple getNextTuple() throws TexeraException {
        if (cursor == CLOSED) {
            return null;
        }
        Tuple.Builder tupleBuilder = new Tuple.Builder();

        result.forEach((k,v) -> tupleBuilder.add(k, AttributeType.INTEGER, v));

        return tupleBuilder.build();

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

    /**
     * Collects ALL the tuples to an in-memory list.
     *
     * @return a list of tuples
     * @throws TexeraException
     */
    public List<Tuple> collectAllTuples() throws TexeraException {
        this.open();
        ArrayList<Tuple> results = new ArrayList<>();
        this.processTuples();
        results.add(this.getNextTuple());
        return results;
    }

    public Schema transformToOutputSchema(Schema... inputSchema) throws DataflowException {
        throw new TexeraException(ErrorMessages.INVALID_OUTPUT_SCHEMA_FOR_SINK);
    }
}
