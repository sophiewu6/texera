import { Injectable } from '@angular/core';
import { Observable, Subject } from 'rxjs/Rx';
import '../../../common/rxjs-operators.ts';

import { WorkflowModelService } from '../model/workflow-model.service';
import { OperatorUIElementService } from '../../operator-ui-element/operator-ui-element.service';


@Injectable()
export class WorkflowUiControlService {

  public _addOperatorSubject = new Subject<OperatorPredicate>();

  public _addLinkSubject = new Subject<OperatorLink>();

  public _deleteOperatorSubject = new Subject<OperatorPredicate>();

  public _deleteLinkSubject = new Subject<OperatorLink>();

  constructor(private workflowModelService: WorkflowModelService,
    private operatorUIElementService: OperatorUIElementService) {

  }

  addOperator(operator: OperatorPredicate, xOffset: number, yOffset: number): void {
    // get operaotr UI element and change its position
    const operatorUIElement = this.operatorUIElementService.getOperatorUIElement(operator.operatorType);
    // set ID
    operatorUIElement.set('id', operator.operatorID);
    // set position
    operatorUIElement.position(
      xOffset - this.workflowModelService.uiPaper.pageOffset().x,
      yOffset - this.workflowModelService.uiPaper.pageOffset().y);

    // add the operator UI element to the UI model
    this.workflowModelService.uiGraph.addCell(operatorUIElement);
    this._addOperatorSubject.next(operator);
  }

  // API for adding link externally (not from user UI)
  addLink(link: OperatorLink): void {
    // TODO: finish this function when write loading a plan to frontend
  }

  // API for adding link externally (not from user UI)
  deleteOperator(operatorID: string): void {
  }

  // API for adding link externally (not from user UI)
  deleteLink(link: OperatorLink): void {
  }

}
