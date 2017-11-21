import { Injectable } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import { Observable, Subject } from 'rxjs/Rx';
import '../../../common/rxjs-operators.ts';

import { WorkflowDataService } from '../current-workflow/workflow-data.service';
import { MOCK_RESULT_DATA } from './mock-result-data';
import { AppSettings } from '../../../common/app-setting';
import { WorkflowLogicalPlan } from '../../model/workflow-logical-plan';

export const EXECUTE_WORKFLOW_ENDPOINT = 'queryplan/execute';

@Injectable()
export class ExecuteWorkflowService {

  constructor(private workflowDataService: WorkflowDataService, private http: HttpClient) { }

  private onExecuteStartedSubject = new Subject<string>();
  executeStarted$ = this.onExecuteStartedSubject.asObservable();

  private onExecuteFinishedSubject = new Subject<Object[]>();
  executeFinished$ = this.onExecuteFinishedSubject.asObservable();

  executeWorkflow(): void {
    console.log('execute!');
    console.log(this.workflowDataService.workflowLogicalPlan);
    this.executeRealWorkflow();
  }

  private executeMockWorkflow(): void {
    this.onExecuteStartedSubject.next('started');
    this.onExecuteFinishedSubject.next(MOCK_RESULT_DATA);
  }

  private executeRealWorkflow(): void {
    const body = this.getLogicalPlanRequest(this.workflowDataService.workflowLogicalPlan);
    console.log(body);
    console.log(JSON.stringify(body));
    this.http.post(`${AppSettings.API_ENDPOINT}/${EXECUTE_WORKFLOW_ENDPOINT}`, body).subscribe(
      value => this.handleExecuteResult(value),
      error => this.handleExecuteError(error)
    );
  }

  private handleExecuteResult(value: any): void {
    if (value && value['code'] === 0) {
      this.onExecuteFinishedSubject.next(value['result']);
    } else {
      this.onExecuteFinishedSubject.error(value);
    }
  }

  private handleExecuteError(error: any): void {
    this.onExecuteFinishedSubject.error(error['message']);
  }

  private getLogicalPlanRequest(logicalPlan: WorkflowLogicalPlan): Object {
    const logicalPlanJson = {'operators': [], 'links': []};
    logicalPlan.operatorPredicates.forEach(
      op => logicalPlanJson['operators'].push(
        Object.assign(
          {'operatorID': op.operatorID, 'operatorType': op.operatorType},
          op.operatorProperties
        )
      ));
    logicalPlanJson['links'] = logicalPlan.operatorLinks;
    return logicalPlanJson;
  }

}
