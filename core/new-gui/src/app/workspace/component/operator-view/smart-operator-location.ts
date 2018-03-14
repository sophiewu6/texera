import { WorkflowJointGraphService } from '../../service/workflow-graph/model/workflow-joint-graph.service';

import * as dagre from 'dagre';
import { WorkflowTexeraGraphService } from '../../service/workflow-graph/model/workflow-texera-graph.service';

export class SmartOperatorLocation {

    private g = new dagre.graphlib.Graph();

    constructor(
        private workflowJointGraphService: WorkflowJointGraphService,
        private workflowTexeraGraphService: WorkflowTexeraGraphService
    ) { }

    public suggestNextLocation() {



    }



}
