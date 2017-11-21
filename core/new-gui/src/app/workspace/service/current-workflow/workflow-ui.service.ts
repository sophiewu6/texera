import { Injectable } from '@angular/core';
import { Observable, Subject } from 'rxjs/Rx';

import { WorkflowDataService } from './workflow-data.service';

@Injectable()
export class WorkflowUIService {

  currentSelectedOperator: string = undefined;

  constructor(private workflowDataService: WorkflowDataService) { }

  private operatorSelectedSubject = new Subject<string>();
  operatorSelected$ = this.operatorSelectedSubject.asObservable();

  selectOperator(operatorID: string): void {
    if (! this.workflowDataService.workflowLogicalPlan.hasOperator(operatorID)) {
      return;
    }
    this.currentSelectedOperator = operatorID;
    this.operatorSelectedSubject.next(operatorID);
  }

}
