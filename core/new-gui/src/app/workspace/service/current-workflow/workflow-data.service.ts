import { Injectable } from '@angular/core';

import { Observable, Subject } from 'rxjs/Rx';
import '../../../common/rxjs-operators.ts';

declare var $: JQueryStatic;
import * as _ from 'lodash';
import * as backbone from 'backbone';
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

  constructor(private operatorUIElementService: OperatorUIElementService) { }

  registerWorkflowPaper(workflowPaper: joint.dia.Paper) {
    this.workflowPaper = workflowPaper;
  }

  private operatorAddedSubject = new Subject<[OperatorPredicate, joint.shapes.basic.Rect]>();
  operatorAdded$ = this.operatorAddedSubject.asObservable();

  addOperator(xOffset: number, yOffset: number, operatorType: string, operatorPredicate?: OperatorPredicate) {
    const operatorID = (++this.maxOperatorID).toString();

    // add operator to workflow data model
    this.workflowData.addOperator(operatorID, operatorType, operatorPredicate);

    // get operaotr UI element and change its position
    const operatorUIElement = this.operatorUIElementService.getOperatorUIElement(operatorType);
    operatorUIElement.position(xOffset - this.workflowPaper.pageOffset().x, yOffset - this.workflowPaper.pageOffset().y);
    // add the operator UI element to the UI model
    this.workflowUI.addCell(operatorUIElement);

    this.operatorAddedSubject.next([this.workflowData.operatorIDMap[operatorID], operatorUIElement]);
  }



}
