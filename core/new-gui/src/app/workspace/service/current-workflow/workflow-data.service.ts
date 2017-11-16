import { Injectable } from '@angular/core';

import { Observable, Subject } from 'rxjs/Rx';
import '../../../common/rxjs-operators.ts';

import * as joint from 'jointjs';

import { OperatorPredicate } from '../../model/operator-predicate';
import { OperatorLink } from '../../model/operator-link';
import { WorkflowLogicalPlan } from '../../model/workflow-logical-plan';

import { OperatorUIElementService } from '../operator-ui-element/operator-ui-element.service';

/* tslint:disable: member-ordering */
@Injectable()
export class WorkflowDataService {

  maxOperatorID = 0;

  workflowData = new WorkflowLogicalPlan();

  workflowUI = new joint.dia.Graph();
  workflowPaper: joint.dia.Paper = undefined;

  currentSelectedOperator: string = undefined;

  constructor(private operatorUIElementService: OperatorUIElementService) { }

  registerWorkflowPaper(workflowPaper: joint.dia.Paper): void {
    this.workflowPaper = workflowPaper;
  }

  private operatorAddedSubject = new Subject<[OperatorPredicate, joint.dia.Cell]>();
  operatorAdded$ = this.operatorAddedSubject.asObservable();

  addOperator(xOffset: number, yOffset: number, operatorType: string, operatorPredicate?: OperatorPredicate): void {
    this.maxOperatorID++;
    const operatorID = 'operator-' + this.maxOperatorID.toString();

    if (! operatorPredicate) {
      operatorPredicate = new OperatorPredicate(operatorID, operatorType, {});
    }

    // add operator to workflow data model
    this.workflowData.addOperator(operatorID, operatorType, operatorPredicate);

    // get operaotr UI element and change its position
    const operatorUIElement = this.operatorUIElementService.getOperatorUIElement(operatorID, operatorType);
    operatorUIElement.position(xOffset - this.workflowPaper.pageOffset().x, yOffset - this.workflowPaper.pageOffset().y);
    // add the operator UI element to the UI model
    this.workflowUI.addCell(operatorUIElement);

    this.operatorAddedSubject.next([operatorPredicate, operatorUIElement]);

    this.selectOperator(operatorID);
  }

  operatorPropertyChangedSubject = new Subject<Object>();


  private operatorSelectedSubject = new Subject<[OperatorPredicate, joint.dia.Cell]>();
  operatorSelected$ = this.operatorSelectedSubject.asObservable();

  selectOperator(operatorID: string): void {
    if (operatorID === this.currentSelectedOperator) {
      return;
    }
    this.currentSelectedOperator = operatorID;
    this.operatorSelectedSubject.next([this.workflowData.operatorIDMap[operatorID], this.workflowUI.getCell(operatorID)]);
  }



}
