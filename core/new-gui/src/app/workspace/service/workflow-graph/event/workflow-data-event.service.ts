import { Injectable } from '@angular/core';
import { Observable } from 'rxjs/Rx';
import '../../../common/rxjs-operators.ts';
import { WorkflowUiControlService } from '../controller/workflow-ui-control.service';
import { WorkflowModelService } from '../model/workflow-model.service';

@Injectable()
export class WorkflowDataEventService {

  constructor(private workflowUiControlService: WorkflowUiControlService, private workflowModelService: WorkflowModelService) {
  }





}
