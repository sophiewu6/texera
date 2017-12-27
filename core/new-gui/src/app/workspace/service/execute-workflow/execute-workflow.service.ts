import { Injectable } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import { Observable, Subject } from 'rxjs/Rx';
import '../../../common/rxjs-operators.ts';

import { MOCK_RESULT_DATA } from './mock-result-data';
import { MOCK_WORKFLOW_PLAN } from './mock-workflow-plan';
import { AppSettings } from '../../../common/app-setting';
import { WorkflowLogicalPlan } from '../../model/workflow-logical-plan';
import { OperatorPredicate } from '../../model/operator-predicate';
import { OperatorLink } from '../../model/operator-link';
import { WorkflowModelService } from '../workflow-graph/workflow-model.service';

export const EXECUTE_WORKFLOW_ENDPOINT = 'queryplan/execute';

@Injectable()
export class ExecuteWorkflowService {

  constructor(private workflowModelService: WorkflowModelService, private http: HttpClient) { }

  // observable and subject for execution start
  private onExecuteStartedSubject = new Subject<string>();
  executeStarted$ = this.onExecuteStartedSubject.asObservable();

  // observable and subject for execution finish
  private onExecuteFinishedSubject = new Subject<Object[]>();
  executeFinished$ = this.onExecuteFinishedSubject.asObservable();

  // main entry function, called when the user requests to execute a workflow
  onExecuteWorkflowRequested(): void {
    console.log('execute workflow plan');
    console.log(this.workflowModelService.logicalPlan);
    this.executeRealPlan();
  }

  // show the mock result data without sending request to server
  private showMockResultData(): void {
    this.onExecuteStartedSubject.next('started');
    this.onExecuteFinishedSubject.next(MOCK_RESULT_DATA);
  }

  // send a mock workflow plan to the server: ScanSource(twitter_sample) -> ViewResults
  private executeMockPlan(): void {
    this.executeWorkflowPlan(MOCK_WORKFLOW_PLAN);
  }

  // send the real workflow plan to the server
  private executeRealPlan(): void {
    this.executeWorkflowPlan(this.workflowModelService.logicalPlan);
  }

  // handle sending a HTTP request of a workflow plan to the server
  private executeWorkflowPlan(workflowPlan: WorkflowLogicalPlan): void {
    const body = this.getLogicalPlanRequest(workflowPlan);
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

  // transform a LogicalPlan object to the HTTP request body according to the backend API
  private getLogicalPlanRequest(logicalPlan: WorkflowLogicalPlan): Object {
    const logicalPlanJson = { 'operators': [], 'links': [] };
    logicalPlan.operatorPredicates.forEach(
      op => logicalPlanJson['operators'].push(
        Object.assign(
          { 'operatorID': op.operatorID, 'operatorType': op.operatorType },
          op.operatorProperties
        )
      ));
    logicalPlanJson['links'] = logicalPlan.operatorLinks;
    return logicalPlanJson;
  }

}
