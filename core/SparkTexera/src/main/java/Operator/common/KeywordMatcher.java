package Operator.common;

import Operator.Base.OperatorBase;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.skewness;

/**
 * @author yuranyan
 */

public class KeywordMatcher extends OperatorBase {

    private final KeywordPredicate predicate;

    public KeywordMatcher(KeywordPredicate predicate) {
        this.predicate = predicate;
    }


    //Substring Match
    private Dataset<Row> keywordMatch(Dataset<Row> dataFrame, String keyword){
        for(String attributeName: predicate.getAttributeNames()){
            //dataFrame = dataFrame.filter((FilterFunction<Row>) x -> x.getAs(attributeName).toString().contains(keyword));
            //dataFrame = dataFrame.filter(col(attributeName).like("%" + keyword + "%"));
            dataFrame = dataFrame.filter(col(attributeName).contains(keyword));
        }
        return dataFrame;
    }

    @Override
    public void execute(){
        addOutput(keywordMatch(getInput().get(0), predicate.getQuery()));
        super.execute();
    }
}
