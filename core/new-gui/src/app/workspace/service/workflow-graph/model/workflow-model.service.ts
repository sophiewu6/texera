import { Injectable } from '@angular/core';

import * as joint from 'jointjs';

import { WorkflowLogicalPlan } from '../../../model/workflow-logical-plan';

/*
  WorkflowModelService contains the global logicalPlan and uiGraph object of the current workflow
  Important: logicalPlan and uiGraph is only allowed to be accessed, not modified.
    All other components and services other than services inside workflow-graph folder
      can only call "get" methods of them.
*/
@Injectable()
export class WorkflowModelService {

  private nextAvailableID = 0;

  logicalPlan = new WorkflowLogicalPlan([], []);

  uiGraph = new joint.dia.Graph();
  uiPaper: joint.dia.Paper = undefined;

  constructor() { }

  // register the workflow paper to the service
  registerWorkflowPaper(workflowPaper: joint.dia.Paper): void {
    this.uiPaper = workflowPaper;
  }

  // generate a new operator ID
  getNextAvailableID(): string {
    this.nextAvailableID++;
    return 'operator-' + this.nextAvailableID.toString();
  }

  // return a new OperatorPredicate with a new ID and default intial properties
  getNewOperatorPredicate(operatorType: string): OperatorPredicate {
    return {
      'operatorID': this.getNextAvailableID(),
      'operatorType': operatorType,
      'operatorProperties': {}
    };
  }

}
