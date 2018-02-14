import { Injectable } from '@angular/core';
import { Observable, Subject } from 'rxjs/Rx';
import '../../../common/rxjs-operators.ts';

import { WorkflowModelService } from './workflow-model.service';

import { WorkflowLogicalPlan } from '../../model/workflow-logical-plan';
import { Operator } from 'rxjs/Operator';
import { OperatorUIElementService } from '../operator-ui-element/operator-ui-element.service';

@Injectable()
export class WorkflowUIChangeService {

  private operatorAddedSubject = new Subject<OperatorPredicate>();
  operatorAdded$ = this.operatorAddedSubject.asObservable();

  private operatorDeletedSubject = new Subject<string>();
  operatorDeleted$ = this.operatorDeletedSubject.asObservable();

  private linkAddedSubject = new Subject<OperatorLink>();
  linkAdded$ = this.linkAddedSubject.asObservable();

  private currentSelectedOperator: string = undefined;
  private operatorSelectedSubject = new Subject<string>();
  operatorSelected$ = this.operatorSelectedSubject.asObservable();

  constructor(private workflowModelService: WorkflowModelService, private operatorUIElementService: OperatorUIElementService) {
    // register callback function for on link added from UI
    this.workflowModelService.uiGraph.on('change:source change:target', (event) => {
      const originID: string = event.get('source').id;
      const destID: string = event.get('target').id;
      if (originID && destID) {
        this.onLinkAddedFromUI({
          'origin': originID,
          'destination': destID
        });
      }
    });
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
    this.operatorAddedSubject.next(operator);
  }


  deleteOperator(operatorID: string): void {
    this.operatorDeletedSubject.next(operatorID);
  }

  // API for adding link externally (not from user UI)
  addLink(link: OperatorLink): void {
    // TODO: finish this function when write loading a plan to frontend
    this.linkAddedSubject.next(link);
  }

  private onLinkAddedFromUI(link: OperatorLink): void {
    if (!(this.workflowModelService.logicalPlan.hasOperator(link.origin)
      && this.workflowModelService.logicalPlan.hasOperator(link.destination))) {
      return;
    }
    this.workflowModelService.logicalPlan.addLink(link);
    this.linkAddedSubject.next(link);
  }

  selectOperator(operatorID: string): void {
    if (!this.workflowModelService.logicalPlan.hasOperator(operatorID)) {
      return;
    }
    this.currentSelectedOperator = operatorID;
    this.operatorSelectedSubject.next(operatorID);
  }

}
