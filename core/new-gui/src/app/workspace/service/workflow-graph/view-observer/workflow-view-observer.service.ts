import { Injectable } from '@angular/core';
import { Observable, Subject } from 'rxjs/Rx';
import '../../../common/rxjs-operators.ts';
import { WorkflowViewEventService } from '../view-event/workflow-view-event.service';
import { OperatorDragDropService } from '../../operator-drag-drop/operator-drag-drop.service';
import { WorkflowModelActionService } from '../model-action/workflow-model-action.service';
import { WorkflowModelService } from '../model/workflow-model.service';

@Injectable()
export class WorkflowViewObserverService {

  constructor(
    private workflowModelService: WorkflowModelService,
    private workflowViewEventService: WorkflowViewEventService,
    private operatorDragDropSerivce: OperatorDragDropService,
    private workflowModelActionService: WorkflowModelActionService
  ) {
    // handle operator is dropped
    this.operatorDragDropSerivce.operatorDroppedInEditor.subscribe(
      (value) => this.handleOperatorDropped(value.operatorType, value.offset.x, value.offset.y));

    // handle delete operator is clicked
    this.workflowViewEventService.deleteOperatorClickedInEditor.subscribe(this.handleDeleteOperatorClicked);
  }

  private handleOperatorDropped(operatorType: string, xOffset: number, yOffset: number) {
    this.workflowModelActionService.addOperator(this.workflowModelService.getNewOperatorPredicate(operatorType), xOffset, yOffset);
  }

  private handleDeleteOperatorClicked(operatorID: string): void {

  }

}
