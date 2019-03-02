package Operator.common.Aggregator;

import Operator.Base.OperatorBase;

import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;



/**
 * @author yinanzhou
 */

public class Aggregator extends OperatorBase {

    private final AggregatorPredicate predicate;

    public Aggregator(AggregatorPredicate predicate) {
        this.predicate = predicate;
    }

    //perform aggregation
    private Dataset<Row> aggregate(Dataset<Row> dataFrame, List<AggregationAttributeAndResult> aggregationItems){
        return dataFrame.agg(Map("_c8" -> "max"));
    }
    
    @Override
    public void execute(){
        addOutput(aggregate(getInput().get(0), predicate.getAttributeAggregateResultList()));
        super.execute();
    }
}
