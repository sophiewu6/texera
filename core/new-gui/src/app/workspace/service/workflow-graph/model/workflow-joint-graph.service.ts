import { Injectable } from '@angular/core';

import * as joint from 'jointjs';

@Injectable()
export class WorkflowJointGraphService {

  public uiGraph = new joint.dia.Graph();
  public uiPaper: joint.dia.Paper = null;

  constructor() { }

  // register the workflow paper to the service
  registerWorkflowPaper(workflowPaper: joint.dia.Paper): void {
    this.uiPaper = workflowPaper;
  }

}
