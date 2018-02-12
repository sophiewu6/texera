import { Injectable, ElementRef } from '@angular/core';

import { Observable, Subject } from 'rxjs/Rx';

import { OperatorUIElementService } from '../operator-ui-element/operator-ui-element.service';

declare var jQuery: JQueryStatic;
import * as joint from 'jointjs';
import { WorkflowUIChangeService } from '../workflow-graph/workflow-ui-change.service';
import { WorkflowModelService } from '../workflow-graph/workflow-model.service';

@Injectable()
export class OperatorDragDropService {

  private registeredOperatorLabelMap = new Map<string, string>();
  private currentOperatorType = '';

  constructor(
    private workflowModelSerivce: WorkflowModelService,
    private workflowUIChangeSerivce: WorkflowUIChangeService,
    private operatorUIElementService: OperatorUIElementService) {
  }

  // register drag for the operator label
  registerDrag(dragElementID: string, operatorType: string) {
    this.registeredOperatorLabelMap.set(dragElementID, operatorType);

    // register callback functions for jquery UI
    jQuery('#' + dragElementID).draggable({
        helper: () => this.createNewOperatorUIElement(operatorType)
    });
  }

  // register drop for the workflow editor area
  registerDrop(dropElementID) {
    jQuery('#' + dropElementID).droppable({
      drop: (event, ui) => this.onOperatorDrop(event, ui)
    });
  }

  private createNewOperatorUIElement(operatorType: string): JQuery<HTMLElement> {
    this.currentOperatorType = operatorType;

    // create a temporary ghost element
    jQuery('body').append('<div id="flyPaper" style="position:fixed;z-index:100;;pointer-event:none;"></div>');

    // get the UI element of the operator
    const operatorUIElement = this.operatorUIElementService.getOperatorUIElement(operatorType);

    // create the jointjs model and paper of the ghost element
    const tempGhostModel = new joint.dia.Graph();

    const tempGhostPaper = new joint.dia.Paper({
      el: jQuery('#flyPaper'),
      width: 120,
      height: 50,
      model: tempGhostModel,
      gridSize: 1
    });

    tempGhostModel.addCell(operatorUIElement);

    return jQuery('#flyPaper');
  }

  private onOperatorDrop(event: Event, ui: JQueryUI.DroppableEventUIParam): void {
    const operatorPredicate = this.workflowModelSerivce.getNewOperatorPredicate(this.currentOperatorType);
    const operatorID = this.workflowUIChangeSerivce.addOperator(
      operatorPredicate, ui.offset.left, ui.offset.top);
    this.workflowUIChangeSerivce.selectOperator(operatorPredicate.operatorID);
  }

}
