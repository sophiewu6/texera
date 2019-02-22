package Operator.common.Compare;

import Operator.Base.OperatorBase;

import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;



/**
 * @author yinanzhou
 */

public class ComparableMatcher extends OperatorBase {

    private final ComparablePredicate predicate;

    public ComparableMatcher(ComparablePredicate predicate) {
        this.predicate = predicate;
    }

    //perform conparison
    private Dataset<Row> compare(Dataset<Row> dataFrame, ComparisonType comparisonType , int compareToValue){
    	String attributeName = predicate.getAttributeName();
    	// return dataFrame;
    	switch (comparisonType) {
	        case EQUAL_TO:
	        	return dataFrame.filter((FilterFunction<Row>) x -> Integer.valueOf(x.getAs(attributeName).toString()) == compareToValue);
	        case GREATER_THAN:
	        	return dataFrame.filter((FilterFunction<Row>) x -> Integer.valueOf(x.getAs(attributeName).toString()) > compareToValue);
	        case GREATER_THAN_OR_EQUAL_TO:
	        	return dataFrame.filter((FilterFunction<Row>) x -> Integer.valueOf(x.getAs(attributeName).toString()) >= compareToValue);
	        case LESS_THAN:
	        	return dataFrame.filter((FilterFunction<Row>) x -> Integer.valueOf(x.getAs(attributeName).toString()) < compareToValue);
	        case LESS_THAN_OR_EQUAL_TO:
	        	return dataFrame.filter((FilterFunction<Row>) x -> Integer.valueOf(x.getAs(attributeName).toString()) <= compareToValue);
	        case NOT_EQUAL_TO:
	        	return dataFrame.filter((FilterFunction<Row>) x -> Integer.valueOf(x.getAs(attributeName).toString()) != compareToValue);
	        default:
	        	return dataFrame;
	        }
    }
    
    @Override
    public void execute(){
        addOutput(compare(getInput().get(0), predicate.getComparisonType(), (int) predicate.getCompareToValue()));
        super.execute();
    }
}
