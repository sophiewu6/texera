import { Injectable } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import { Observable, Subject } from 'rxjs/Rx';
import '../../../common/rxjs-operators.ts';

import { MOCK_RESULT_DATA } from './mock-result-data';
import { MOCK_WORKFLOW_PLAN } from './mock-workflow-plan';
import { AppSettings } from '../../../common/app-setting';
import { WorkflowTexeraGraphService } from '../workflow-graph/model/workflow-texera-graph.service';

export const EXECUTE_WORKFLOW_ENDPOINT = 'queryplan/execute';

@Injectable()
export class ExecuteWorkflowService {

  constructor(private workflowTexeraGraphService: WorkflowTexeraGraphService, private http: HttpClient) { }

  // observable and subject for execution start
  private onExecuteStartedSubject = new Subject<string>();
  executeStarted$ = this.onExecuteStartedSubject.asObservable();

  // observable and subject for execution finish
  private onExecuteFinishedSubject = new Subject<Object[]>();
  executeFinished$ = this.onExecuteFinishedSubject.asObservable();

  // main entry function, called when the user requests to execute a workflow
  onExecuteWorkflowRequested(): void {
    console.log('execute workflow plan');
    console.log(this.workflowTexeraGraphService.texeraWorkflowGraph);
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
    this.executeWorkflowPlan(this.workflowTexeraGraphService.texeraWorkflowGraph);
  }

  // handle sending a HTTP request of a workflow plan to the server
  private executeWorkflowPlan(workflowPlan: WorkflowGraphReadonly): void {
    const body = this.getLogicalPlanRequest(workflowPlan);
    console.log('making http post request to backend');
    console.log('body is:');
    console.log(body);
    this.http.post(`${AppSettings.API_ENDPOINT}/${EXECUTE_WORKFLOW_ENDPOINT}`, JSON.stringify(body),
      {headers: {'Content-Type': 'application/json'}}).subscribe(
        response => this.handleExecuteResult(response),
        errorResponse => this.handleExecuteError(errorResponse)
    );
  }

  private handleExecuteResult(response: any): void {
    console.log('handling success result ');
    console.log('result value is:');
    console.log(response);
    this.onExecuteFinishedSubject.next(response);
  }

  private handleExecuteError(errorResponse: any): void {
    console.log('handling error result ');
    console.log('error value is:');
    console.log(errorResponse);
    this.onExecuteFinishedSubject.next(errorResponse.error);
  }

  // transform a LogicalPlan object to the HTTP request body according to the backend API
  private getLogicalPlanRequest(logicalPlan: WorkflowGraphReadonly): Object {
    const logicalPlanJson = { operators: [], links: [] };

    logicalPlan.getOperators().forEach(
      op => logicalPlanJson.operators.push(
        Object.assign(
          { operatorID: op.operatorID, operatorType: op.operatorType },
          op.operatorProperties
        )
      ));

    // filter out the non-connected links
    logicalPlan.getLinks()
    .filter(link => (link.sourceOperator && link.targetOperator))
    .forEach(
      link => logicalPlanJson.links.push(
        Object.assign(
          {origin: link.sourceOperator, destination: link.targetOperator}
        )
      )
    );
    return logicalPlanJson;
  }

}
