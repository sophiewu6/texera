package edu.uci.ics.texera.dataflow.plangen.schemaPropagation;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.module.jsonSchema.JsonSchemaGenerator;
import com.fasterxml.jackson.module.jsonSchema.factories.JsonSchemaProducer;
import com.fasterxml.jackson.module.jsonSchema.types.ObjectSchema;
import com.github.fge.jsonschema.core.exceptions.ProcessingException;
import com.github.fge.jsonschema.core.report.ProcessingReport;
import com.github.fge.jsonschema.main.JsonSchema;
import com.github.fge.jsonschema.main.JsonSchemaFactory;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableMap;
import edu.uci.ics.texera.api.exception.TexeraException;
import edu.uci.ics.texera.api.schema.Schema;
import edu.uci.ics.texera.dataflow.common.JsonSchemaHelper;
import edu.uci.ics.texera.dataflow.common.PredicateBase;
import edu.uci.ics.texera.dataflow.common.PropertyNameConstants;
import edu.uci.ics.texera.dataflow.plangen.LogicalPlan;
import edu.uci.ics.texera.dataflow.plangen.OperatorLink;

import java.io.IOException;
import java.nio.file.Files;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static edu.uci.ics.texera.dataflow.plangen.schemaPropagation.OperatorValidationResult.fromError;
import static edu.uci.ics.texera.dataflow.plangen.schemaPropagation.OperatorValidationResult.fromOperator;

public class SchemaPropagation {

    /* Example of using /autocomplete end point (how this inline update method works):

        1. At the beginning of creating a graph, (for example) when a scan source and a keyword search
            operators are initailized (dragged in the flow-chart) but unlinked, the graph looks like this:
             ___________________                         ___________________
            |   Source: Scan    |                       |  Keyword Search   |
            |   TableName: N/A  |                       |  Attributes: N/A  |
            |___________________|                       |___________________|

        2. Then, you can feel free to link these two operators together, or go ahead and select a
            table as the source first. Let's link them together first.
             ___________________                         ___________________
            |  Source: Scan     |                       |   Keyword Search  |
            |  TableName: N/A   | ====================> |  Attributes: N/A  |
            |___________________|                       |___________________|

        3. At this moment, the Keyword Search operator still does NOT have any available options for
            its Attributes field because of the lack of the source. Therefore, we can select a table
            name as the source next (let's use table "tweet" as an example here)
             ___________________                         ___________________
            |  Source: Scan     |                       |  Keyword Search   |
            | TableName: tweet  | ====================> |  Attributes: N/A  |
            |___________________|                       |___________________|

        4. After select table "tweet" as the source, now you can see the options list of Attributes in
            the Keyword Search operator becomes available. you should see 4 options in the list: name,
            description, logicPlan, payload. Feel free to choose whichever you need for your desired result.
             ___________________                         ___________________
            |  Source: Scan     |                       |  Keyword Search   |
            | TableName: tweet  | ====================> |  Attributes: text |
            |___________________|                       |___________________|

        5. Basically, the method supports that whenever you connect a source (with a valid table name)
            to a regular search operator, the later operator is able to recognize the metadata of its
            input operator (which is the source), and then updates its attribute options in the drop-down
            list. To illustrate how powerful this functionality is, you can add a new (Scan) Source and
            pick another table which is different than table "tweet" we have already created. The graph
            now should be looked like the following:

             ___________________                         ___________________
            |  Source: Scan     |                       |  Keyword Search   |
            | TableName: tweet  | ====================> |  Attributes: text |
            |___________________|                       |___________________|
             ___________________
            | Source: Scan      |
            | TableName: news   |
            |___________________|

        6. Then, connect "news" to the Keyword Search operator. The original link between "tweet"
            and Keyword Search will automatically disappear.
             ___________________                         ___________________
            |  Source: Scan     |                       |  Keyword Search   |
            | TableName: tweet  |       //============> |  Attributes: N/A  |
            |___________________|     //                |___________________|
             ___________________    //
            | Source: Scan      | //
            | TableName: news   |
            |___________________|

        7. After the new link generated, the Attributes field of the Keyword Search will be empty again. When
            you try to check its drop-down list, the options are all updated to dictionary's attributes, which
            are name and payload. The options from "tweet" are all gone.

    */


    public static Map<String, Map<String, Schema>> schemaPropagation(WorkflowUnvalidated workflowUnvalidated) {

        LogicalPlan validPartialLogicalPlan = getValidPartialLogicalPlan(workflowUnvalidated);
        // Get all input schema for valid operator with valid links
        Map<String, Schema> outputSchema = validPartialLogicalPlan.retrieveAllOperatorOutputSchema();
        return getAvailableSchemasForOperators(workflowUnvalidated, outputSchema);

    }

    private static Map<String, Map<String, Schema>> getAvailableSchemasForOperators(
            WorkflowUnvalidated workflowUnvalidated, Map<String, Schema> outputSchemas) {

        ArrayListMultimap<String, String> inverseAdjacencyList = ArrayListMultimap.create();
        workflowUnvalidated.getLinks().forEach(link -> inverseAdjacencyList.put(link.getDestination(), link.getOrigin()));

        Map<String, Map<String, Schema>> availableSchemas = new HashMap<>();

        inverseAdjacencyList.keySet().forEach(operator -> {
            List<String> inputOperators = inverseAdjacencyList.get(operator);

            if (inputOperators.isEmpty()) {
                availableSchemas.put(operator, ImmutableMap.of(operator, outputSchemas.get(operator)));
            } else {
                ImmutableMap.Builder<String, Schema> builder = ImmutableMap.builder();
                inverseAdjacencyList.get(operator).forEach(input -> builder.put(input, outputSchemas.get(input)));
                availableSchemas.put(operator, builder.build());
            }
        });
        
        return availableSchemas;
    }

    /**
     * Used for automatic schema propagation as user is building the graph.
     *
     * Retrieve all the valid operator and links and store into node validOperators and validLinks
     *
     * A operator is valid if the json representation of the operator can pass all the test in the specific
     * operator predicate constructor and can successfully be casted into a PredicateBase object (without throwing any error).
     * The standard of valid is different from operator to operator and it can be seen in the definition of the
     * constructor of a specific operator predicate.
     * An example of invalid operator is a KeywordPredicate has empty query
     *
     * A valid link would be a link that connects two valid operators
     */
    private static LogicalPlan getValidPartialLogicalPlan(WorkflowUnvalidated workflowUnvalidated) {

        LogicalPlan logicalPlan = new LogicalPlan();

        for (OperatorUnvalidated operator : workflowUnvalidated.getOperators()) {
            OperatorValidationResult operatorValidationResult = validateOperator(operator);
            if (operatorValidationResult.getOperatorPredicate() != null) {
                logicalPlan.addOperator(operatorValidationResult.getOperatorPredicate());
            }
        }

        for (OperatorLink link : workflowUnvalidated.getLinks()) {
            if (logicalPlan.hasOperator(link.getOrigin()) && logicalPlan.hasOperator(link.getDestination())) {
                logicalPlan.addLink(link);
            }
        }

        return logicalPlan;
    }

    private static OperatorValidationResult validateOperator(OperatorUnvalidated operator) {
        ObjectNode objectNode  = new ObjectMapper().createObjectNode();
        objectNode.put("operatorID", operator.getOperatorID());
        objectNode.put("operatorType", operator.getOperatorType());
        operator.getOperatorProperties().forEach((key, value) -> objectNode.putPOJO(key, value));

        Class<? extends PredicateBase> predicateClass = JsonSchemaHelper.operatorTypeMap.inverse().get(operator.getOperatorType());
        if (predicateClass == null) {
            return fromError("operator type " + operator.getOperatorType() + " doesn't exist");
        }

        JsonSchema jsonSchema;
        try {
            jsonSchema = JsonSchemaFactory.byDefault().getJsonSchema(new ObjectMapper().readTree(
                    Files.readAllBytes(JsonSchemaHelper.getJsonSchemaPath(predicateClass))));

        } catch (IOException | ProcessingException e) {
            return fromError("operator schema " + operator.getOperatorType() + " doesn't exist");
        }

        ProcessingReport report = jsonSchema.validateUnchecked(objectNode, true);

        if (! report.isSuccess()) {
            return fromError(report.toString());
        }

        PredicateBase operatorPredicate;
        try {
            operatorPredicate = new ObjectMapper().treeToValue(objectNode, PredicateBase.class);
        } catch (JsonProcessingException e) {
            return fromError(e.getMessage());
        }

        return fromOperator(operatorPredicate);
    }


}
