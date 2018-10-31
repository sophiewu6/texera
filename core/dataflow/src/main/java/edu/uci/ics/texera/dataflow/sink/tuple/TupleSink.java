package edu.uci.ics.texera.dataflow.sink.tuple;

import java.util.ArrayList;
import java.util.List;

import edu.uci.ics.texera.api.constants.ErrorMessages;
import edu.uci.ics.texera.api.constants.SchemaConstants;
import edu.uci.ics.texera.api.dataflow.IOperator;
import edu.uci.ics.texera.api.dataflow.ISink;
import edu.uci.ics.texera.api.exception.DataflowException;
import edu.uci.ics.texera.api.exception.TexeraException;
import edu.uci.ics.texera.api.schema.Schema;
import edu.uci.ics.texera.api.tuple.Tuple;
import edu.uci.ics.texera.dataflow.lineage.DatabaseConnector;
import edu.uci.ics.texera.dataflow.source.asterix.AsterixSource;

/**
 * TupleStreamSink is a sink that can be used by the caller to get tuples one by one.
 * 
 * @author Zuozhi Wang
 *
 */
public class TupleSink implements ISink {
	private String operatorName;
    private String previousOperatorName;
    private int outputID=1;
	
    private TupleSinkPredicate predicate;
    
    private IOperator inputOperator;
    
    private Schema inputSchema;
    private Schema outputSchema;
    
    private int cursor = CLOSED;

    /**
     * TupleStreamSink is a sink that can be used to
     *   collect tuples to an in-memory list.
     *
     * TupleStreamSink removes the payload attribute
     *   from the schema and each tuple.
     *
     */
    public TupleSink() {
        this(new TupleSinkPredicate());
    }
    
    public TupleSink(TupleSinkPredicate predicate) {
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
                .removeIfExists(SchemaConstants.PAYLOAD, AsterixSource.RAW_DATA).build();
        cursor = OPENED;
    }

    @Override
    public void processTuples() throws TexeraException {
        return;
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
        if(resultTuple!=null) {
            if(outputID==1) {
                String className=this.getClass().getName();
                this.operatorName=className.substring(className.lastIndexOf(".")+1,className.length());
                String previousClassName=inputOperator.getClass().getName();
                this.previousOperatorName=previousClassName.substring(previousClassName.lastIndexOf(".")+1,previousClassName.length());
                Schema schema=getOutputSchema();
                DatabaseConnector.createResultTable(this.operatorName+"Result", schema);
                DatabaseConnector.createLineageTable(operatorName+"Lineage");
                DatabaseConnector.deleteTupleFromResultCatalogTable(operatorName);
                DatabaseConnector.deleteTupleFromLineageCatalogTable(operatorName);
                DatabaseConnector.insertTupleIntoResultCatalogTable(operatorName);
                DatabaseConnector.insertTupleIntoLineageCatalogTable(operatorName);
            }
            Tuple newTuple=new Tuple.Builder(resultTuple).removeIfExists(SchemaConstants.PAYLOAD, AsterixSource.RAW_DATA).build();
            String tupleContent=newTuple.toString().replaceAll("'", "''");
            int inputID=DatabaseConnector.selectInputIDFromResultTable(tupleContent, previousOperatorName+"Result");
            DatabaseConnector.insertTupleToResultTable(operatorName+"Result", outputID, tupleContent);
            DatabaseConnector.insertTupleIntoLineageTable(operatorName+"Lineage", inputID, outputID);
            outputID++;
        }
        return new Tuple.Builder(resultTuple)
                .removeIfExists(SchemaConstants.PAYLOAD, AsterixSource.RAW_DATA).build();

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
        Tuple tuple;
        while ((tuple = this.getNextTuple()) != null) {
            results.add(tuple);
        }
        this.close();
        return results;
    }

    public Schema transformToOutputSchema(Schema... inputSchema) throws DataflowException {
        throw new TexeraException(ErrorMessages.INVALID_OUTPUT_SCHEMA_FOR_SINK);
    }
}
