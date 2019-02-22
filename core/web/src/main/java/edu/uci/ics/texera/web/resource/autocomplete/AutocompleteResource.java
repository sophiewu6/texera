package edu.uci.ics.texera.web.resource.autocomplete;

import com.fasterxml.jackson.databind.JsonNode;

import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

@Path("/autocomplete")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class AutocompleteResource {


    @POST
    @Path("/execute")
    // TODO: investigate how to use LogicalPlan directly
    public JsonNode executeQueryPlan(WorkflowUnvalidated workflowUnvalidated) {





        return null;
    }

}
