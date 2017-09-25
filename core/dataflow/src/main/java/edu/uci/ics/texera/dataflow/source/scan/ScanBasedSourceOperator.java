package edu.uci.ics.texera.dataflow.source.scan;

import edu.uci.ics.texera.api.exception.DataflowException;
import edu.uci.ics.texera.api.exception.StorageException;
import edu.uci.ics.texera.api.exception.TexeraException;
import edu.uci.ics.texera.api.schema.Schema;
import edu.uci.ics.texera.api.tuple.Tuple;

import org.apache.lucene.search.MatchAllDocsQuery;

import edu.uci.ics.texera.api.constants.ErrorMessages;
import edu.uci.ics.texera.api.dataflow.ISourceOperator;
import edu.uci.ics.texera.storage.DataReader;
import edu.uci.ics.texera.storage.RelationManager;

/**
 * Created by chenli on 3/28/16.
 */
public class ScanBasedSourceOperator implements ISourceOperator {

	private static final int MaxOutput = 100000;
	private int outputTupleCounter = 0;
	
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
        	if(++outputTupleCounter > MaxOutput){
        		return null;
        	}
//        	if(outputTupleCounter % 1000 == 0){
//        		System.out.println(outputTupleCounter);
//        	}
            return dataReader.getNextTuple();
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
        return dataReader.getOutputSchema();
    }
}
