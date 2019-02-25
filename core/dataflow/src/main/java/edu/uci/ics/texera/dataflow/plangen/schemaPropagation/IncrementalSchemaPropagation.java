package edu.uci.ics.texera.dataflow.plangen.schemaPropagation;

import com.google.common.collect.ImmutableMap;
import edu.uci.ics.texera.api.schema.Schema;
import edu.uci.ics.texera.dataflow.plangen.OperatorLink;

import java.util.List;
import java.util.Map;

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



    public IncrementalSchemaPropagation() {

    }


    public void acceptCommand(WorkflowCommand command, WorkflowCommandPayload payload) {
        // get the payload based on the command type
        switch (command) {
            case ADD_OPERATOR:
                AddOperatorPayload addOperatorPayload = (AddOperatorPayload) payload;
                break;
            case DELETE_OPERATOR:
                // TODO cast payload
                break;
            case ADD_LINK:
                break;
            case DELETE_LINK:
                break;
            case SET_OPERATOR_PROPERTY:
                break;
        }
    }

    public Map<String, Schema> collectAllAvailableSchemas() {

        return null;
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
