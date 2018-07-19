package edu.uci.ics.texera.dataflow.resource.dictionary;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import edu.uci.ics.texera.api.exception.TexeraException;
import edu.uci.ics.texera.api.tuple.Tuple;
import edu.uci.ics.texera.dataflow.sink.tuple.TupleSink;
import edu.uci.ics.texera.dataflow.source.tuple.TupleSourceOperator;
import edu.uci.ics.texera.dataflow.resource.dictionary.DictionaryManager;;

public class DictionaryManagerTest {
    
    /*
     * Test connection
     */
    @Test
    public void connectionTest() throws TexeraException {
        DictionaryManager.getInstance();
    }
    
}
