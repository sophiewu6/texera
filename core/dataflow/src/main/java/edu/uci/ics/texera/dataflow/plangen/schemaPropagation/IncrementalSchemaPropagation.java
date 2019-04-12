package edu.uci.ics.texera.dataflow.plangen.schemaPropagation;

import com.google.common.collect.ImmutableMap;

import edu.uci.ics.texera.api.dataflow.IOperator;
import edu.uci.ics.texera.api.dataflow.ISourceOperator;
import edu.uci.ics.texera.api.exception.DataflowException;
import edu.uci.ics.texera.api.schema.Schema;
import edu.uci.ics.texera.dataflow.plangen.schemaPropagation.SchemaPropagation;
import edu.uci.ics.texera.dataflow.common.PredicateBase;
import edu.uci.ics.texera.dataflow.plangen.OperatorLink;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class IncrementalSchemaPropagation {

    public interface WorkflowCommandPayload {

    }

    public static class AddOperatorPayload implements WorkflowCommandPayload {
        private OperatorUnvalidated operatorUnvalidated;

        public AddOperatorPayload(OperatorUnvalidated operatorUnvalidated) {
            this.operatorUnvalidated = operatorUnvalidated;
        }

        public OperatorUnvalidated getOperatorUnvalidated() {
            return operatorUnvalidated;
        }
    }

    public static class DeleteOperatorPayload implements WorkflowCommandPayload {
        private String operatorID;

        public DeleteOperatorPayload(String operatorID) {
            this.operatorID = operatorID;
        }

        public String getOperatorID() {
            return operatorID;
        }
    }

    public static class AddLinkPayload implements WorkflowCommandPayload {
        private OperatorLink link;

        public AddLinkPayload(OperatorLink link) {
            this.link = link;
        }

        public OperatorLink getLink() {
            return link;
        }
    }

    public static class DeleteLinkPayload implements WorkflowCommandPayload {
        private OperatorLink link;

        public DeleteLinkPayload(OperatorLink link) {
            this.link = link;
        }

        public OperatorLink getLink() {
            return link;
        }
    }

    public static class SetOperatorPropertyPayload implements WorkflowCommandPayload {
        private OperatorUnvalidated operatorUnvalidated;

        public SetOperatorPropertyPayload(OperatorUnvalidated operatorUnvalidated) {
            this.operatorUnvalidated = operatorUnvalidated;
        }

        public OperatorUnvalidated getOperatorUnvalidated() {
            return operatorUnvalidated;
        }
    }



    public enum WorkflowCommand {
        ADD_OPERATOR,
        DELETE_OPERATOR,
        ADD_LINK,
        DELETE_LINK,
        SET_OPERATOR_PROPERTY
    }


    
    // a map from operatorID to its operator
    private HashMap<String, IOperator> operatorObjectMap;
    // a map from operatorID to its schema
    private Map<String, Schema> schemaCollection;
    // a map of an operator ID to outputs (a set of operator IDs)
    private LinkedHashMap<String, LinkedHashSet<String>> parentList;
    // a map of an operator ID to inputs (a set of operator IDs)
    private LinkedHashMap<String, LinkedHashSet<String>> childList;
    

    public IncrementalSchemaPropagation() {
    	operatorObjectMap = new HashMap<String, IOperator>();
    	schemaCollection = new HashMap<String, Schema>();
    	parentList = new LinkedHashMap<>();
    	childList = new LinkedHashMap<>();
    }
    
    
    
    public void updateOperatorObjectMap(OperatorValidationResult op) {
    	
    	PredicateBase pb = op.getOperatorPredicate();
    	String obID = pb.getID();
    	
    	if (!operatorObjectMap.containsKey(obID)) {
    		operatorObjectMap.put(obID, pb.newOperator());
    	}	
    }
    
    public void addRelation(OperatorLink link) {
    	String origin = link.getOrigin();
    	String destination = link.getDestination();
    	// TODO: update parentList and childList
    }
    
    public void deleteRelation(OperatorLink link) {
    	String origin = link.getOrigin();
    	String destination = link.getDestination();
    	// TODO: update parentList and childList
    }
    
    public void updateOperatorProperty(OperatorValidationResult op) {
    	// TODO: update operation property
    }
    
    public Schema getSchema(String obID) {
    	// return Schema if Schema from all parents are available in schemaCollection,
    	// otherwise return null
    	
    	IOperator ob = operatorObjectMap.get(obID);
    	
		if (ob instanceof ISourceOperator)
    		return ob.getOutputSchema();
		
    	else {
    		List<Schema> inputSchema = new ArrayList<Schema>();
    		for (String parentID: parentList.get(obID)) {
    			if (schemaCollection.get(parentID) == null)
    				return null;
    			inputSchema.add(schemaCollection.get(parentID));
    		}
    		return ob.transformToOutputSchema(inputSchema.toArray(new Schema[inputSchema.size()]));
    	}
    }

    public void acceptCommand(WorkflowCommand command, WorkflowCommandPayload payload) throws DataflowException {
        // get the payload based on the command type
        switch (command) {
            case ADD_OPERATOR:
                AddOperatorPayload addOperatorPayload = (AddOperatorPayload) payload;
                OperatorUnvalidated addOperator = addOperatorPayload.getOperatorUnvalidated();
                OperatorValidationResult addOP = SchemaPropagation.validateOperator(addOperator);
                if (addOP.getErrorMessage() != null)
                	throw new DataflowException(addOP.getErrorMessage());
                updateOperatorObjectMap(addOP);
                break;
            case DELETE_OPERATOR:
            	DeleteOperatorPayload deleteOperatorPayload = (DeleteOperatorPayload) payload;
            	String deleteOperatorID = deleteOperatorPayload.getOperatorID();
            	operatorObjectMap.remove(deleteOperatorID);
            	schemaCollection.remove(deleteOperatorID);
                break;
            case ADD_LINK:
            	AddLinkPayload addLinkPayload = (AddLinkPayload) payload;
            	OperatorLink addLink = addLinkPayload.getLink();
            	addRelation(addLink);
                break;
            case DELETE_LINK:
            	DeleteLinkPayload deleteLinkPayload = (DeleteLinkPayload) payload;
            	OperatorLink deleteLink = deleteLinkPayload.getLink();
            	deleteRelation(deleteLink);
                break;
            case SET_OPERATOR_PROPERTY:
            	SetOperatorPropertyPayload setOperatorPropertyPayload = (SetOperatorPropertyPayload) payload;
            	OperatorUnvalidated setOperatorProperty = setOperatorPropertyPayload.getOperatorUnvalidated();
            	OperatorValidationResult setOP = SchemaPropagation.validateOperator(setOperatorProperty);
            	if (setOP.getErrorMessage() != null)
                	throw new DataflowException(setOP.getErrorMessage());
                updateOperatorProperty(setOP);
                break;
        }
    }

    public Map<String, Schema> collectAllAvailableSchemas() {
    	// return a map from operatorID to its schema
        return schemaCollection;
    }


    public static void main(String[] args) throws Exception {
        // for testing

        IncrementalSchemaPropagation incrementalSchemaPropagation = new IncrementalSchemaPropagation();

        // add add operator command
        AddOperatorPayload addOperatorPayload1 = new AddOperatorPayload(new OperatorUnvalidated(
                "1", "ScanSource", ImmutableMap.of("tableName", "twitter_sample")
        ));

        incrementalSchemaPropagation.acceptCommand(WorkflowCommand.ADD_OPERATOR, addOperatorPayload1);

    }




}
