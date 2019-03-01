package edu.uci.ics.texera.dataflow.source.mysql;

import java.util.List;

import edu.uci.ics.texera.api.tuple.Tuple;
import edu.uci.ics.texera.dataflow.sink.tuple.TupleSink;
import edu.uci.ics.texera.dataflow.sink.tuple.TupleSinkPredicate;

public class ReadMysqlTest {
	
	public static void main(String[] args) throws Exception {
		ReadMysqlPredicate predicate = new ReadMysqlPredicate(
				"localhost", 3306, "movie_search", "sales", "", "", "root", "1a2b3c4d5e!");
		
		ReadMysqlOperator operator = predicate.newOperator();
		
		TupleSink tupleSink = new TupleSinkPredicate(10, 0).newOperator();
		
		
		tupleSink.setInputOperator(operator);
		
		tupleSink.open();
		
		List<Tuple> results = tupleSink.collectAllTuples();
		
		tupleSink.close();
		
	
		
	}

}
