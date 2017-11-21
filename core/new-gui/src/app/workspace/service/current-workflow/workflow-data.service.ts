import { Injectable } from '@angular/core';

import { Observable, Subject } from 'rxjs/Rx';
import '../../../common/rxjs-operators.ts';

import * as joint from 'jointjs';

import { OperatorPredicate } from '../../model/operator-predicate';
import { OperatorLink } from '../../model/operator-link';
import { WorkflowLogicalPlan } from '../../model/workflow-logical-plan';

import { OperatorUIElementService } from '../operator-ui-element/operator-ui-element.service';

@Injectable()
export class WorkflowDataService {

  maxOperatorID = 0;

  workflowLogicalPlan = new WorkflowLogicalPlan();

  workflowUI = new joint.dia.Graph();
  workflowPaper: joint.dia.Paper = undefined;

  private operatorAddedSubject = new Subject<[OperatorPredicate, joint.dia.Cell]>();
  operatorAdded$ = this.operatorAddedSubject.asObservable();

  private linkAddedSubject = new Subject<OperatorLink>();
  linkAdded$ = this.linkAddedSubject.asObservable();

  private operatorPropertyChangedSubject = new Subject<[string, OperatorPredicate]>();
  operatorPropertyChanged$ = this.operatorPropertyChangedSubject.asObservable();

  constructor(private operatorUIElementService: OperatorUIElementService) {
    this.workflowUI.on('change:source change:target', (link) => {
      const originID: string = link.get('source').id;
      const destID: string = link.get('target').id;
      if (originID && destID) {
        this.onLinkAdded(originID, destID);
      }
    });
  }

  registerWorkflowPaper(workflowPaper: joint.dia.Paper): void {
    this.workflowPaper = workflowPaper;
  }

  addOperator(xOffset: number, yOffset: number, operatorType: string, operatorPredicate?: OperatorPredicate): string {
    this.maxOperatorID++;
    const operatorID = 'operator-' + this.maxOperatorID.toString();

    if (!operatorPredicate) {
      operatorPredicate = new OperatorPredicate(operatorID, operatorType, []);
    }

    // add operator to workflow data model
    this.workflowLogicalPlan.addOperator(operatorID, operatorType, operatorPredicate);

    // get operaotr UI element and change its position
    const operatorUIElement = this.operatorUIElementService.getOperatorUIElement(operatorID, operatorType);
    operatorUIElement.position(xOffset - this.workflowPaper.pageOffset().x, yOffset - this.workflowPaper.pageOffset().y);
    // add the operator UI element to the UI model
    this.workflowUI.addCell(operatorUIElement);

    this.operatorAddedSubject.next([operatorPredicate, operatorUIElement]);

    return operatorID;
  }

  changeOperatorProperty(operatorID: string, properties: Object[]): void {
    this.workflowLogicalPlan.getOperator(operatorID).operatorProperties = properties;
    this.operatorPropertyChangedSubject.next([operatorID, this.workflowLogicalPlan.getOperator(operatorID)]);
  }

  private onLinkAdded(origin: string, dest: string): void {
    if (! (this.workflowLogicalPlan.hasOperator(origin) && this.workflowLogicalPlan.hasOperator(dest))) {
      return;
    }
    const operatorLink = new OperatorLink(origin, dest);
    this.workflowLogicalPlan.addLink(operatorLink);
    this.linkAddedSubject.next(operatorLink);
  }

}
