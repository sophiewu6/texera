import { Injectable } from '@angular/core';
import { Observable, Subject } from 'rxjs/Rx';
import '../../../common/rxjs-operators.ts';

import { WorkflowModelService } from './workflow-model.service';
import { WorkflowUIChangeService } from './workflow-ui-change.service';
import { OperatorPredicate } from '../../model/operator-predicate';
import { OperatorLink } from '../../model/operator-link';

@Injectable()
export class WorkflowDataChangeService {

  private propertyChangedSubject = new Subject<OperatorPredicate>();
  propertyChanged$ = this.propertyChangedSubject.asObservable();

  constructor(private workflowModelSerivce: WorkflowModelService,
    private workflowUIChangeService: WorkflowUIChangeService) {
      this.workflowUIChangeService.operatorAdded$.subscribe((operator) => this.addOperator(operator));
      this.workflowUIChangeService.linkAdded$.subscribe((link) => this.addLink(link));
    }


  private addOperator(operator: OperatorPredicate): void {
    this.workflowModelSerivce.logicalPlan.addOperator(operator);
  }

  private addLink(link: OperatorLink): void {
    this.workflowModelSerivce.logicalPlan.addLink(link);
  }

  changeProperty(operatorID: string, properties: Object): void {
    if (! this.workflowModelSerivce.logicalPlan.hasOperator(operatorID)) {
      return;
    }
    this.workflowModelSerivce.logicalPlan.getOperator(operatorID).operatorProperties = properties;
    this.propertyChangedSubject.next(this.workflowModelSerivce.logicalPlan.getOperator(operatorID));
  }

}
