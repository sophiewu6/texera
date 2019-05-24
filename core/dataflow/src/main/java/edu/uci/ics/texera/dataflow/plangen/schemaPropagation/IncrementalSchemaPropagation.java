package edu.uci.ics.texera.dataflow.plangen.schemaPropagation;

import com.google.common.collect.ImmutableMap;

import edu.uci.ics.texera.api.dataflow.IOperator;
import edu.uci.ics.texera.api.dataflow.ISourceOperator;
import edu.uci.ics.texera.api.exception.DataflowException;
import edu.uci.ics.texera.api.schema.Schema;
import edu.uci.ics.texera.dataflow.plangen.schemaPropagation.SchemaPropagation;
import edu.uci.ics.texera.dataflow.common.PredicateBase;
import edu.uci.ics.texera.dataflow.plangen.OperatorLink;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
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
    // a map of an operator ID to upstream (a set of operator IDs)
    private LinkedHashMap<String, LinkedHashSet<String>> parentList;
    // a map of an operator ID to downstream (a set of operator IDs)
    private LinkedHashMap<String, LinkedHashSet<String>> childList;
    

    public IncrementalSchemaPropagation() {
    	operatorObjectMap = new HashMap<String, IOperator>();
    	schemaCollection = new HashMap<String, Schema>();
    	parentList = new LinkedHashMap<>();
    	childList = new LinkedHashMap<>();
    }
    
    
    
    public void propagateSchemaUpdate(String obID) {
    	// recursively update the schema downstream
    	if (childList.containsKey(obID)) {
    		for (String childID : childList.get(obID)) {
    			
    			Schema tempSchema = getSchema(childID);
    			if (tempSchema == null) continue;
    			
    			schemaCollection.put(childID, tempSchema);
				propagateSchemaUpdate(childID);
    		}
    	}
    }
    
    public void propagateSchemaDelete(String obID) {
    	// recursively delete the schema downstream
    	if (childList.containsKey(obID)) {
    		for (String childID : childList.get(obID)) {
    			propagateSchemaDelete(childID);
    			schemaCollection.remove(childID);
    		}
    	}
    }
    
    public void updateOperatorObjectMap(OperatorValidationResult op, String obID) {
    	
    	PredicateBase pb = op.getOperatorPredicate();
    	IOperator ob = pb.newOperator();
    	
		operatorObjectMap.put(obID, ob);

    	Schema tempSchema = getSchema(obID);
    	if (tempSchema != null)
    		schemaCollection.put(obID, tempSchema);
    	
    	// propagate schema
    	propagateSchemaUpdate(obID);
    }
    
    public void addRelation(OperatorLink link) {
    	
    	String origin = link.getOrigin();
    	String destination = link.getDestination();
    	
    	// update parentList
    	if (!parentList.containsKey(destination)) {
    		parentList.put(destination, new LinkedHashSet<String>());
    	}
    	parentList.get(destination).add(origin);
    	
    	// update childList
    	if (!childList.containsKey(origin)) {
    		childList.put(origin, new LinkedHashSet<String>());
    	}
    	childList.get(origin).add(destination);
    	
    	// propagate schema
    	Schema tempSchema = getSchema(destination);
		if (tempSchema != null) {
			schemaCollection.put(destination, tempSchema);
			propagateSchemaUpdate(destination);
		}
    }
    
    public void deleteRelation(OperatorLink link) {
    	
    	String origin = link.getOrigin();
    	String destination = link.getDestination();
    	
    	// update parentList
    	if (parentList.containsKey(destination)) {
    		parentList.get(destination).remove(origin);
    	}
    	
    	// update childList
    	if (childList.containsKey(origin)) {
    		childList.get(origin).remove(destination);
    	}
    	
    	// propagate schema
    	schemaCollection.remove(destination);
    	propagateSchemaDelete(destination);
    }
    
//    public void updateOperatorProperty(OperatorValidationResult op, String obID) {
//    	PredicateBase pb = op.getOperatorPredicate();
//    	IOperator ob = pb.newOperator();
//
//    	this.operatorObjectMap.get(obID);
//    	
//    
//    	Schema tempSchema = getSchema(obID);
//    	if (tempSchema != null)
//    		schemaCollection.put(obID, tempSchema);
//    	
//    	// propagate schema
//    	propagateSchemaUpdate(obID);
//    }
    
    public Schema getSchema(String obID) {
    	// return Schema if Schema from all parents are available in schemaCollection,
    	// otherwise return null
    	
    	IOperator ob = operatorObjectMap.get(obID);
    	
		if (ob instanceof ISourceOperator) {
			ob.open();
    		return ob.getOutputSchema();
		}
		
    	else {
    		List<Schema> inputSchema = new ArrayList<Schema>();
    		if (!parentList.containsKey(obID)) return null;
    		for (String parentID: parentList.get(obID)) {
    			if (schemaCollection.get(parentID) == null)
    				return null;
    			inputSchema.add(schemaCollection.get(parentID));
    			try {
    				IOperator parentOp = operatorObjectMap.get(parentID);
    				Class<?> pclass = ob.getClass();
    				Method setInputOperator = pclass.getDeclaredMethod("setInputOperator", IOperator.class);
					setInputOperator.invoke(ob, parentOp);
    			} catch (Exception e) {
    				throw new DataflowException(e);
    			}
    		}
    		ob.open();
    		return ob.transformToOutputSchema(inputSchema.toArray(new Schema[inputSchema.size()]));    		
    	}
    }

    public void acceptCommand(WorkflowCommand command, WorkflowCommandPayload payload) throws DataflowException {
        // get the payload based on the command type
    	String obID = "";
        switch (command) {
            case ADD_OPERATOR:
                AddOperatorPayload addOperatorPayload = (AddOperatorPayload) payload;
                OperatorUnvalidated addOperator = addOperatorPayload.getOperatorUnvalidated();
                obID = addOperator.getOperatorID();
                OperatorValidationResult addOP = SchemaPropagation.validateOperator(addOperator);
                if (addOP.getErrorMessage() != null)
                	throw new DataflowException(addOP.getErrorMessage());
                updateOperatorObjectMap(addOP, obID);
                break;
            case DELETE_OPERATOR:
            	DeleteOperatorPayload deleteOperatorPayload = (DeleteOperatorPayload) payload;
            	String deleteOperatorID = deleteOperatorPayload.getOperatorID();
            	operatorObjectMap.remove(deleteOperatorID);
            	schemaCollection.remove(deleteOperatorID);
            	propagateSchemaDelete(deleteOperatorID);
                break;
            case ADD_LINK:
            	// TODO: update all schema downstream
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
            	obID = setOperatorProperty.getOperatorID();
            	OperatorValidationResult setOP = SchemaPropagation.validateOperator(setOperatorProperty);
            	if (setOP.getErrorMessage() != null) {
            		schemaCollection.remove(obID);
            		propagateSchemaDelete(obID);
                	throw new DataflowException(setOP.getErrorMessage());
            	}
            	updateOperatorObjectMap(setOP, obID);
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
        
        AddOperatorPayload addOperatorPayload2 = new AddOperatorPayload(new OperatorUnvalidated(
                "2", "NlpSentiment", ImmutableMap.of("attribute", "text", "resultAttribute", "sentimentResult")
        ));
        incrementalSchemaPropagation.acceptCommand(WorkflowCommand.ADD_OPERATOR, addOperatorPayload2);
        
        AddOperatorPayload addOperatorPayload3 = new AddOperatorPayload(new OperatorUnvalidated(
                "3", "ViewResults", ImmutableMap.of()
        ));
        incrementalSchemaPropagation.acceptCommand(WorkflowCommand.ADD_OPERATOR, addOperatorPayload3);

        incrementalSchemaPropagation.acceptCommand(WorkflowCommand.ADD_LINK, new AddLinkPayload(new OperatorLink("1", "2")));
        
        incrementalSchemaPropagation.acceptCommand(WorkflowCommand.ADD_LINK, new AddLinkPayload(new OperatorLink("2", "3")));
        
        Map<String, Schema> c1 = incrementalSchemaPropagation.collectAllAvailableSchemas();
        
        incrementalSchemaPropagation.acceptCommand(WorkflowCommand.DELETE_LINK, new DeleteLinkPayload(new OperatorLink("2", "3")));

        Map<String, Schema> c2 = incrementalSchemaPropagation.collectAllAvailableSchemas();
        
        incrementalSchemaPropagation.acceptCommand(WorkflowCommand.DELETE_LINK, new DeleteLinkPayload(new OperatorLink("1", "2")));
        
        Map<String, Schema> c3 = incrementalSchemaPropagation.collectAllAvailableSchemas();
        
        incrementalSchemaPropagation.acceptCommand(WorkflowCommand.ADD_LINK, new AddLinkPayload(new OperatorLink("1", "3")));

        Map<String, Schema> c4 = incrementalSchemaPropagation.collectAllAvailableSchemas();

        
        SetOperatorPropertyPayload setPropertyPayload1 = new SetOperatorPropertyPayload(new OperatorUnvalidated(
                "2", "NlpSentiment", ImmutableMap.of("attribute", "text", "resultAttribute", "abc")));
        
        incrementalSchemaPropagation.acceptCommand(WorkflowCommand.SET_OPERATOR_PROPERTY, setPropertyPayload1);
        
        

        System.out.println("hi");
        Map<String, Schema> c = incrementalSchemaPropagation.collectAllAvailableSchemas();
        System.out.println(c);
        System.out.println("hi");

    }




}
