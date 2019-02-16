package Operator.Base;

import Exception.TexeraException;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.ArrayList;
import java.util.List;

/**
 * @author yuranyan
 */

public abstract class OperatorBase implements IOperator {

    private List<Dataset<Row>> inputDatasetList = new ArrayList<>();
    private List<Dataset<Row>> outputDatasetList = new ArrayList<>();

    protected SparkSession sparkSession;

    private List<OperatorBase> outputOperatorList = new ArrayList<>();

    //Operations about the input dataset list

    protected void setInput(List<Dataset<Row>> inputDatasetList){
        this.inputDatasetList = inputDatasetList;
    }

    protected List<Dataset<Row>> getInput(){
        return inputDatasetList;
    }


    //Operations about the output dataset list

    protected void setOutput(List<Dataset<Row>> outputDatasetList) {
        this.outputDatasetList = outputDatasetList;
    }

    protected void changeOutput(Dataset<Row> dataFrame, int index){ this.outputDatasetList.set(index, dataFrame);}

    protected void addOutput(Dataset<Row> dataFrame){
        this.outputDatasetList.add(dataFrame);
    }

    protected List<Dataset<Row>> getOutput() {
        return outputDatasetList;
    }


    //Other operations

    public void addOutputOperator(OperatorBase operator){
        outputOperatorList.add(operator);
    }

    public void bindSparkSession(SparkSession sparkSession){
        this.sparkSession = sparkSession;
    }

    /**
     * Each operator needs to pass its output to all the output operators, except the sink operator
     * @throws TexeraException
     */

    public void execute() throws TexeraException {
        for(OperatorBase operator: outputOperatorList)
            operator.setInput(getOutput());
    };

}
