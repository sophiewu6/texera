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
