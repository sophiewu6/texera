import { Injectable } from '@angular/core';

import * as joint from 'jointjs';

import { WorkflowLogicalPlan } from '../../model/workflow-logical-plan';
import { OperatorPredicate } from '../../model/operator-predicate';

@Injectable()
export class WorkflowModelService {

  private nextAvailableID = 0;

  logicalPlan = new WorkflowLogicalPlan();

  uiGraph = new joint.dia.Graph();
  uiPaper: joint.dia.Paper = undefined;

  constructor() { }

  registerWorkflowPaper(workflowPaper: joint.dia.Paper): void {
    this.uiPaper = workflowPaper;
  }

  getNextAvailableID(): string {
    this.nextAvailableID++;
    return 'operator-' + this.nextAvailableID.toString();
  }

  // return a new OperatorPredicate with a new ID and default intial properties
  getNewOperatorPredicate(operatorType: string): OperatorPredicate {
    return new OperatorPredicate(this.getNextAvailableID(), operatorType, {});
  }

}
