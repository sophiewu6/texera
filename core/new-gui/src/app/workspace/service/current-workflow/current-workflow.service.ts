import { Injectable } from '@angular/core';

import { Observable, Subject } from 'rxjs/Rx';
import '../../../common/rxjs-operators.ts';

declare var $: JQueryStatic;
import * as _ from 'lodash';
import * as backbone from 'backbone';
import * as joint from 'jointjs';

import OperatorPredicate from '../../model/operator-predicate';
import OperatorLink from '../../model/operator-link';
import WorkflowLogicalPlan from '../../model/workflow-logical-plan';
import WorkflowUIGraph from '../../model/workflow-ui-graph';

/* tslint:disable: member-ordering */
@Injectable()
export class CurrentWorkflowService {

  maxOperatorID = 0;

  workflowData = new WorkflowLogicalPlan();

  workflowUI = new joint.dia.Graph();
  workflowPaper: joint.dia.Paper;

  registerWorkflowPaper(workflowPaper: joint.dia.Paper) {
    this.workflowPaper = workflowPaper;
  }

  private operatorAddedSubject = new Subject<OperatorPredicate>();
  operatorAdded$ = this.operatorAddedSubject.asObservable();
  addOperator(operatorType: string, operatorPredicate?: OperatorPredicate) {
    const operatorID = (++this.maxOperatorID).toString();

    this.workflowData.addOperator(operatorID, operatorType, operatorPredicate);

    this.operatorAddedSubject.next(this.workflowData.operatorIDMap[operatorID]);
  }


  constructor() { }



}
