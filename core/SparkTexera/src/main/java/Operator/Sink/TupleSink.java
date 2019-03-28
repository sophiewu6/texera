package Operator.Sink;

import Operator.Base.OperatorBase;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.List;

public class TupleSink extends OperatorBase {

    private final TupleSinkPredicate predicate;

    public TupleSink(TupleSinkPredicate predicate) {this.predicate = predicate;}

    public void execute(){
        setOutput(getInput());
    }

    public ArrayNode transferToTuple(){

        ArrayNode arrayNode = new ObjectMapper().createArrayNode();

        for(Dataset<Row> output: getInput()) {
//            List<Row> outputTuple = output.limit(predicate.getLimit()).collectAsList();
//            for (Row row : outputTuple) {
//                ObjectNode objectNode = new ObjectMapper().createObjectNode();
//                for (String column : row.schema().fieldNames()) {
//                    objectNode.put(column,  (String) row.getAs(column));
//                }
//                arrayNode.add(objectNode);
//            }

            // Change Sink to Count operator
            Long count = output.count();
            ObjectNode objectNode = new ObjectMapper().createObjectNode();
            objectNode.put("Count",count);
            arrayNode.add(objectNode);
        }

        return arrayNode;
    }




}
