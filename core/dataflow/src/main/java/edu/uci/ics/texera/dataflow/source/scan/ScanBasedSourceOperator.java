package edu.uci.ics.texera.dataflow.source.scan;

import edu.uci.ics.texera.api.exception.DataflowException;
import edu.uci.ics.texera.api.exception.StorageException;
import edu.uci.ics.texera.api.exception.TexeraException;
import edu.uci.ics.texera.api.schema.Schema;
import edu.uci.ics.texera.api.tuple.Tuple;
import edu.uci.ics.texera.dataflow.lineage.DatabaseConnector;

import org.apache.lucene.search.MatchAllDocsQuery;

import edu.uci.ics.texera.api.constants.ErrorMessages;
import edu.uci.ics.texera.api.dataflow.ISourceOperator;
import edu.uci.ics.texera.storage.DataReader;
import edu.uci.ics.texera.storage.RelationManager;

/**
 * Created by chenli on 3/28/16.
 */
public class ScanBasedSourceOperator implements ISourceOperator {
	private String operatorName;
    private int lineageTupleID=1;

    private DataReader dataReader;
    
    private boolean isOpen = false;

    public ScanBasedSourceOperator(ScanSourcePredicate predicate) throws DataflowException {
        try {
            this.dataReader = RelationManager.getInstance().getTableDataReader(
                    predicate.getTableName(), new MatchAllDocsQuery());
            // TODO add an option to set if payload is added in the future.
            this.dataReader.setPayloadAdded(true);
        } catch (StorageException e) {
            throw new DataflowException(e);
        }
    }

    @Override
    public void open() throws TexeraException {
        if (isOpen) {
            return;
        }
        try {
            dataReader.open();
            isOpen = true;
        } catch (Exception e) {
            throw new DataflowException(e.getMessage(), e);
        }
    }

    @Override
    public Tuple getNextTuple() throws TexeraException {
        if (! isOpen) {
            throw new DataflowException(ErrorMessages.OPERATOR_NOT_OPENED);
        }
        try {
        	Tuple tuple=dataReader.getNextTuple();
            if(tuple!=null) {
                if(lineageTupleID==1) {
                    String className=this.getClass().getName();
                    this.operatorName=className.substring(className.lastIndexOf(".")+1,className.length());
                    DatabaseConnector.deleteTupleFromResultCatalogTable(operatorName);
                    DatabaseConnector.insertTupleIntoResultCatalogTable(operatorName);
                    Schema schema=getOutputSchema();
                    DatabaseConnector.createResultTable(this.operatorName+"Result", schema);
                }
                Tuple tupleWithLineageID=DatabaseConnector.addTupleLineageIDToOutput(tuple, lineageTupleID);
                String tupleContent=tupleWithLineageID.toString().replaceAll("'", "''");
                DatabaseConnector.insertTupleToResultTable(this.operatorName+"Result", tupleContent);
                lineageTupleID++;
                return tupleWithLineageID;
            }
            return null;
        } catch (Exception e) {
            e.printStackTrace();
            throw new DataflowException(e.getMessage(), e);
        }
    }

    @Override
    public void close() throws TexeraException {
        if (! isOpen) {
            return;
        }
        try {
            dataReader.close();
            isOpen = false;
        } catch (Exception e) {
            throw new DataflowException(e.getMessage(), e);
        }
    }

    @Override
    public Schema getOutputSchema() {
    	Schema schema=dataReader.getOutputSchema();
    	schema=DatabaseConnector.addLineageIDAttribute(schema);
        return schema;
    }

    public Schema transformToOutputSchema(Schema... inputSchema) throws DataflowException {
        if (inputSchema == null || inputSchema.length == 0) {
            if (getOutputSchema() == null) {
                open();
                close();
            }
            return getOutputSchema();
        }
        throw new TexeraException(ErrorMessages.INVALID_INPUT_SCHEMA_FOR_SOURCE);
    }
}
