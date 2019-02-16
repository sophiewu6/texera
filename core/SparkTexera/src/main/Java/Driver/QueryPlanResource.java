package Driver;


import LogicalPlan.LogicalPlan;
import Exception.TexeraException;
import Operator.Base.OperatorBase;
import Operator.Sink.TupleSink;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.spark.sql.SparkSession;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;

/**
 * This class will be the resource class for accepting a query plan edu.uci.ics.texera.web.request and executing the
 * query plan to get the query response
 * Created by kishorenarendran on 10/17/16.
 *
 * @author Kishore
 * @author Zuozhi
 */

@Path("/queryplan")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class QueryPlanResource {

//    public static java.nio.file.Path resultDirectory = Utils.getTexeraHomePath().resolve("query-results");


    public static  void main(String[] args) throws Exception {
        String requestJson = "{\"operators\":[{\"filePath\":\"/Users/yuranyan/Desktop/Study/UCI/CS 121/HW1/1.txt\",\"resultAttribute\":\"text\",\"recursive\":false,\"operatorID\":\"operator-4e5542ae-e029-4644-b5a2-1b81214928d9\",\"operatorType\":\"FileSource\"},{\"attributes\":[\"text\"],\"query\":\"world\",\"matchingType\":\"scan\",\"spanListName\":\"result\",\"operatorID\":\"operator-d7761354-09da-4a6a-92d7-8c4827851455\",\"operatorType\":\"KeywordMatcher\"},{\"limit\":10,\"offset\":0,\"operatorID\":\"operator-fba401d9-7189-474c-b27e-70e474ed95d5\",\"operatorType\":\"ViewResults\"}],\"links\":[{\"origin\":\"operator-4e5542ae-e029-4644-b5a2-1b81214928d9\",\"destination\":\"operator-d7761354-09da-4a6a-92d7-8c4827851455\"},{\"origin\":\"operator-d7761354-09da-4a6a-92d7-8c4827851455\",\"destination\":\"operator-fba401d9-7189-474c-b27e-70e474ed95d5\"}]}\n";

        new QueryPlanResource().executeQueryPlan(requestJson);

    }



    @GET
    @Path("/test")
    public String testApi() {
        return "hello world";
    }



    /**
     * This is the edu.uci.ics.texera.web.request handler for the execution of a Query Plan.
     *
     * @param logicalPlanJson, the json representation of the logical plan
     * @return - Generic TexeraWebResponse object
     */
    @POST
    @Path("/execute")
    // TODO: investigate how to use LogicalPlan directly
    public JsonNode executeQueryPlan(String logicalPlanJson) {
        try {

            System.out.println(logicalPlanJson);
            System.out.println();

            SparkSession sparkSession = SparkSession
                    .builder()
                    .appName("Simple Texera Spark Backend")
                    .config("spark.master", "local[*]")
                    .getOrCreate();

            LogicalPlan logicalPlan = new ObjectMapper().readValue(logicalPlanJson, LogicalPlan.class);

            HashMap<String, OperatorBase> DAG = logicalPlan.buildQueryPlan(sparkSession);
            List<String> order = logicalPlan.getTopological();

            long startTime = System.nanoTime();

            for (String operatorID : order)
                DAG.get(operatorID).execute();

            //Here we assume there is only one sink operator in the workflow, and the sink operator only have one output
            String sinkId = logicalPlan.getSink();

            // send response back to frontend


            // make sure result directory is created
//            if (Files.notExists(resultDirectory)) {
//                Files.createDirectories(resultDirectory);
//            }

            // clean up old result files
            //cleanupOldResults();

            // generate new UUID as the result id
            String resultID = UUID.randomUUID().toString();

            // write original json of the result into a file
            //java.nio.file.Path resultFile = resultDirectory.resolve(resultID + ".json");

            //Files.createFile(resultFile);
            //Files.write(resultFile, new ObjectMapper().writeValueAsBytes(results));

            // put readable json of the result into response
            TupleSink sink = (TupleSink) (DAG.get(sinkId));
            ArrayNode resultNode = sink.transferToTuple();

            System.out.println();
            System.out.println("Spark workflow execution time: " + ((System.nanoTime() - startTime) / Math.pow(10, 9)));
            System.out.println();


            ObjectNode response = new ObjectMapper().createObjectNode();
            response.put("code", 0);
            response.set("result", resultNode);
            response.put("resultID", resultID);
            return response;
//            else {
//                // execute the plan and return success message
//                Engine.getEngine().evaluate(plan);
//                ObjectNode response = new ObjectMapper().createObjectNode();
//                response.put("code", 1);
//                response.put("message", "plan sucessfully executed");
//                return response;
//            }

        } catch (IOException | TexeraException e) {
            e.printStackTrace();
            throw new TexeraException(e.getMessage());
        }
    }
}