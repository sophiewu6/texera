package LogicalPlan;

import Operator.Base.OperatorBase;
import Operator.Base.PredicateBase;
import Utility.PropertyNameConstants;
import Exception.TexeraException;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.spark.sql.SparkSession;

import java.util.*;

/**
 * Spark RDD graph of operators representing the query plan
 * @author Zuozhi Wang
 * @author yuranyan
 */

public class LogicalPlan {


    // a map from operatorID to its operator
    private HashMap<String, OperatorBase> operatorObjectMap;
    // use LinkedHashMap to retain insertion order
    // a map from operatorID to its predicate
    private LinkedHashMap<String, PredicateBase> operatorPredicateMap;
    // a map of an operator ID to operator's outputs (a set of operator IDs)
    private LinkedHashMap<String, LinkedHashSet<String>> adjacencyList;

    // a map of tracing how many incoming edge each vertex has
    private HashMap<String, Integer> inEdgeNum = new HashMap<>();

    // variables used by calculating the topological order
    private HashMap<String,String> parent = new HashMap<>();
    private List<String> topological = new ArrayList<>();
    private HashSet<String> currentSubtree = new HashSet<>();

    private String sinkId;

    /**
     * Create an empty logical plan.
     *
     * This class is not a JSON entry point. It is for internal use only.
     */
    public LogicalPlan() {
        operatorPredicateMap = new LinkedHashMap<>();
        adjacencyList = new LinkedHashMap<>();
    }

    /**
     * Create a LogicalPlan.LogicalPlan from an existing plan (represented by a list of operators and a list of links)
     *
     * @param predicateList, a list of operator predicates
     * @param operatorLinkList, a list of operator links
     */
    @JsonCreator
    public LogicalPlan(
            @JsonProperty(value = PropertyNameConstants.OPERATOR_LIST, required = true)
                    List<PredicateBase> predicateList,
            @JsonProperty(value = PropertyNameConstants.OPERATOR_LINK_LIST, required = true)
                    List<OperatorLink> operatorLinkList
    ) {
        // initialize private variables
        this();
        // add predicates and links
        for (PredicateBase predicate : predicateList) {
            addOperator(predicate);
        }
        for (OperatorLink link : operatorLinkList) {
            addLink(link);
        }
    }


    /**
     * Adds a new operator to the logical plan.
     * @param operatorPredicate, the predicate of the operator
     */
    public void addOperator(PredicateBase operatorPredicate) {

        String operatorID = operatorPredicate.getID();
        if(hasOperator(operatorID))
            throw new TexeraException(String.format("duplicate operator id: %s is found", operatorID));
        operatorPredicateMap.put(operatorID, operatorPredicate);
        adjacencyList.put(operatorID, new LinkedHashSet<>());
    }

    /**
     * Adds a new link to the logical plan
     * @param operatorLink, a link of two operators
     */
    public void addLink(OperatorLink operatorLink) {

        String origin = operatorLink.getOrigin();
        String destination = operatorLink.getDestination();
        if(!hasOperator(origin))
            throw new TexeraException(String.format("origin operator id: %s is not found", origin));
        if(!hasOperator(destination))
            throw new TexeraException(String.format("destination operator id: %s is not found", destination));
        adjacencyList.get(origin).add(destination);

        // add incoming edge number by 1
        if(!inEdgeNum.containsKey(destination))
            inEdgeNum.put(destination,1);
        else
            inEdgeNum.put(destination,inEdgeNum.get(destination) + 1);
    }

    /**
     * Builds and returns the operator DAG
     * @return the plan generated from the operator graph
     * @throws TexeraException, if the operator graph is invalid.
     */
    public HashMap<String,OperatorBase> buildQueryPlan(SparkSession sparkSession) throws TexeraException {

        buildOperators(sparkSession);
        //validateOperatorGraph();
        connectOperators();
        findTopological();


        return operatorObjectMap;
    }

    /**
     * Get the topological order of the execution
     * @return
     */
    public List<String> getTopological(){
        return topological;
    }

    /**
     * Returns true if the operator graph contains the operatorID.
     *
     * @param operatorID
     * @return
     */
    public boolean hasOperator(String operatorID) {
        return adjacencyList.containsKey(operatorID);
    }

    /**
     * Build the operator objects from operator properties.
     * Add SparkSession reference to the source operator
     * This implementation assumes that operators with incoming edge 0 are source operators
     * @param sparkSession the SparkSession object in the driver
     */
    private void buildOperators(SparkSession sparkSession) throws TexeraException {
        operatorObjectMap = new HashMap<>();
        for (String operatorID : operatorPredicateMap.keySet()) {
            OperatorBase operator = operatorPredicateMap.get(operatorID).newOperator();
            if(!inEdgeNum.containsKey(operatorID)){
                operator.bindSparkSession(sparkSession);
            }

            operatorObjectMap.put(operatorID, operator);
        }
    }

    /**
     * This function find a valid topological order for the DAG.
     * If there is a cycle, DAG is invalid and this function throws an error
     * @throws TexeraException
     */
    private void findTopological() throws TexeraException {


        for(String vertex: adjacencyList.keySet()){
            if(operatorObjectMap.get(vertex).getClass().toString().contains("Sink")){
                sinkId = vertex;
            }
            if(!parent.containsKey(vertex)){
                parent.put(vertex,vertex);
                search(vertex);
            }
        }
    }

    /**
     * Helper DFS search function for finding topological ordering
     * @param vertex the vertex that need to be searched by the DFS algorithm
     */
    private void search(String vertex){
        currentSubtree.add(vertex);
        for(String adjacentVertex: adjacencyList.get(vertex)){
            if(!parent.containsKey(adjacentVertex)){
                parent.put(adjacentVertex,vertex);
                search(adjacentVertex);
            }
            else {
                if(currentSubtree.contains(adjacentVertex)){
                    throw new TexeraException("Detect a cycle in workflow");
                }
            }
        }
        topological.add(0,vertex);
        currentSubtree.remove(vertex);
    }

    /*
     * Connects OperatorBase objects together according to the operator graph.
     *
     * This function assumes that the operator graph is valid.
     * It goes through every link, and invokes
     * the corresponding "setInputOperator" function to connect operators.
     */
    private void connectOperators() throws TexeraException {
//        for (String vertex : adjacencyList.keySet()) {
//            IOperator currentOperator = operatorObjectMap.get(vertex);
//            int outputArity = adjacencyList.get(vertex).size();
//
//            // automatically adds a OneToNBroadcastConnector if the output arity > 1
//            if (outputArity > 1) {
//                OneToNBroadcastConnector oneToNConnector = new OneToNBroadcastConnector(outputArity);
//                oneToNConnector.setInputOperator(currentOperator);
//                int counter = 0;
//                for (String adjacentVertex : adjacencyList.get(vertex)) {
//                    IOperator adjacentOperator = operatorObjectMap.get(adjacentVertex);
//                    handleSetInputOperator(oneToNConnector.getOutputOperator(counter), adjacentOperator);
//                    counter++;
//                }
//            } else {
//                for (String adjacentVertex : adjacencyList.get(vertex)) {
//                    IOperator adjacentOperator = operatorObjectMap.get(adjacentVertex);
//                    handleSetInputOperator(currentOperator, adjacentOperator);
//                }
//            }
//        }


        for(String vertex: adjacencyList.keySet()){
            OperatorBase currentOperator = operatorObjectMap.get(vertex);
            for(String adjacentVertex: adjacencyList.get(vertex)){
                OperatorBase adjacentOperator = operatorObjectMap.get(adjacentVertex);
                currentOperator.addOutputOperator(adjacentOperator);
            }
        }


    }

    public String getSink(){
        return sinkId;
    }



}
