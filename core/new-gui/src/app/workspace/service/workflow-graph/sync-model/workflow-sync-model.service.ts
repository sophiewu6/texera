import { Injectable } from '@angular/core';
import { WorkflowModelService } from '../model/workflow-model.service';
import { WorkflowModelEventService } from '../model-event/workflow-model-event.service';

@Injectable()
export class WorkflowSyncModelService {

  constructor(
    private workflowModelService: WorkflowModelService,
    private workflowModelEventService: WorkflowModelEventService
  ) { }

}
