package edu.uci.ics.texera.web.resource.schemaPropagation;


import edu.uci.ics.texera.api.schema.Schema;
import edu.uci.ics.texera.dataflow.plangen.schemaPropagation.SchemaPropagation;
import edu.uci.ics.texera.dataflow.plangen.schemaPropagation.WorkflowUnvalidated;

import javax.ws.rs.POST;
import javax.ws.rs.Path;
import java.util.Map;


public class SchemaPropagationResource {

    @POST
    @Path("/schema-propagation")
    public Map<String, Schema> schemaPropagationApi(WorkflowUnvalidated workflowUnvalidated) {
        return SchemaPropagation.schemaPropagation(workflowUnvalidated);
    }


}
