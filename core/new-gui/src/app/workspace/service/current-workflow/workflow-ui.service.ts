import { Injectable } from '@angular/core';
import { Observable, Subject } from 'rxjs/Rx';
import '../../../common/rxjs-operators.ts';

import * as joint from 'jointjs';
import { OperatorPredicate } from '../../model/operator-predicate';
import { OperatorLink } from '../../model/operator-link';
import { WorkflowLogicalPlan } from '../../model/workflow-logical-plan';

import { OperatorUIElementService } from '../operator-ui-element/operator-ui-element.service';


@Injectable()
export class WorkflowUIService {

  workflowUI = new joint.dia.Graph();
  workflowPaper: joint.dia.Paper = undefined;

  maxOperatorIDNumber = 0;

  currentSelectedOperator: string = undefined;

  private operatorAddedSubject = new Subject<[OperatorPredicate, joint.dia.Cell]>();
  operatorAdded$ = this.operatorAddedSubject.asObservable();

  private linkAddedSubject = new Subject<OperatorLink>();
  linkAdded$ = this.linkAddedSubject.asObservable();

  private operatorSelectedSubject = new Subject<string>();
  operatorSelected$ = this.operatorSelectedSubject.asObservable();

  constructor(private operatorUIElementService: OperatorUIElementService) {
  }

  registerWorkflowPaper(workflowPaper: joint.dia.Paper): void {
    this.workflowPaper = workflowPaper;
  }

  addOperator(xOffset: number, yOffset: number, operatorType: string, operatorPredicate?: OperatorPredicate): string {
    this.maxOperatorIDNumber++;
    const operatorID = 'operator-' + this.maxOperatorIDNumber.toString();

    if (!operatorPredicate) {
      operatorPredicate = new OperatorPredicate(operatorID, operatorType, {});
    }

    // get operaotr UI element and change its position
    const operatorUIElement = this.operatorUIElementService.getOperatorUIElement(operatorID, operatorType);
    operatorUIElement.position(xOffset - this.workflowPaper.pageOffset().x, yOffset - this.workflowPaper.pageOffset().y);
    // add the operator UI element to the UI model
    this.workflowUI.addCell(operatorUIElement);

    this.operatorAddedSubject.next([operatorPredicate, operatorUIElement]);

    return operatorID;
  }

  selectOperator(operatorID: string): void {
    if (this.workflowPaper.getModelById(operatorID).isLink()) {
      return;
    }
    this.currentSelectedOperator = operatorID;
    this.operatorSelectedSubject.next(operatorID);
  }

}
