package Driver;

import Utility.DataConstants;


import Operator.Base.PredicateBase;
import Exception.*;
import Operator.JsonSchemaHelper;
import Operator.OperatorGroupConstants;
import Operator.OperatorGroupConstants.GroupOrder;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;


import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;

@Path("/resources")
@Produces(MediaType.APPLICATION_JSON)
public class SystemResource {

    private ObjectMapper objectMapper = DataConstants.defaultObjectMapper;

    public static class OperatorMetadata {
        @JsonProperty("operators")
        List<JsonNode> operatorSchemaList;
        @JsonProperty("groups")
        List<GroupOrder> operatorGroupList;

        public OperatorMetadata() { }

        public OperatorMetadata(List<JsonNode> operatorSchemaList, List<GroupOrder> operatorGroupList) {
            this.operatorSchemaList = operatorSchemaList;
            this.operatorGroupList = operatorGroupList;
        }
    }

    @GET
    @Path("/operator-metadata")
    public OperatorMetadata getOperatorMetadata() {
        try {

            List<JsonNode> operators = new ArrayList<>();
            for (Class<? extends PredicateBase> predicateClass : JsonSchemaHelper.operatorTypeMap.keySet()) {
                JsonNode schemaNode = objectMapper.readTree(
                        Files.readAllBytes(JsonSchemaHelper.getJsonSchemaPath(predicateClass)));
                operators.add(schemaNode);
            }

            OperatorMetadata operatorMetadata = new OperatorMetadata(operators, OperatorGroupConstants.OperatorGroupOrderList);

            return operatorMetadata;
        } catch (Exception e) {
            e.printStackTrace();
            throw new TexeraWebException(e);
        }
    }

//    @GET
//    @Path("/table-metadata")
//    public TexeraWebResponse getMetadata() throws StorageException, JsonProcessingException {
//        List<TableMetadata> tableMetadata = RelationManager.getInstance().getMetaData();
//        return new TexeraWebResponse(0, new ObjectMapper().writeValueAsString(tableMetadata));
//    }
//
//    /**
//     * Get the list of dictionaries
//     */
//    @GET
//    @Path("/dictionaries")
//    public TexeraWebResponse getDictionaries() throws StorageException, JsonProcessingException {
//        DictionaryManager dictionaryManager = DictionaryManager.getInstance();
//        List<String> dictionaries = dictionaryManager.getDictionaries();
//
//        return new TexeraWebResponse(0, new ObjectMapper().writeValueAsString(dictionaries));
//    }
//
//    /**
//     * Get the content of dictionary
//     */
//    @GET
//    @Path("/dictionary")
//    public TexeraWebResponse getDictionary(@QueryParam("name") String name) {
//        DictionaryManager dictionaryManager = DictionaryManager.getInstance();
//        String dictionaryContent = dictionaryManager.getDictionary(name);
//
//        return new TexeraWebResponse(0, dictionaryContent);
//    }

}