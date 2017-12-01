import { Injectable } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import { Observable, Subject } from 'rxjs/Rx';
import '../../../common/rxjs-operators.ts';

import { MOCK_RESULT_DATA } from './mock-result-data';
import { AppSettings } from '../../../common/app-setting';
import { WorkflowLogicalPlan } from '../../model/workflow-logical-plan';
import { OperatorPredicate } from '../../model/operator-predicate';
import { OperatorLink } from '../../model/operator-link';
import { WorkflowModelService } from '../workflow-graph/workflow-model.service';

export const EXECUTE_WORKFLOW_ENDPOINT = 'queryplan/execute';

@Injectable()
export class ExecuteWorkflowService {

  constructor(private workflowModelService: WorkflowModelService, private http: HttpClient) { }

  private onExecuteStartedSubject = new Subject<string>();
  executeStarted$ = this.onExecuteStartedSubject.asObservable();

  private onExecuteFinishedSubject = new Subject<Object[]>();
  executeFinished$ = this.onExecuteFinishedSubject.asObservable();

  executeWorkflow(): void {
    console.log('execute workflow plan');
    console.log(this.workflowModelService.logicalPlan);
    this.executeRealWorkflow();
  }

  private executeMockWorkflow(): void {
    this.onExecuteStartedSubject.next('started');
    this.onExecuteFinishedSubject.next(MOCK_RESULT_DATA);
  }

  private executeRealWorkflow(): void {
    // const mockLogicalPlan = new WorkflowLogicalPlan();
    // mockLogicalPlan.addOperator('operator-1', 'ScanSource',
    //   new OperatorPredicate('operator-1', 'ScanSource', { 'tableName': 'twitter_sample' }));
    // mockLogicalPlan.addOperator('operator-2', 'ViewResults',
    //   new OperatorPredicate('operator-2', 'ViewResults', { 'limit': 10, 'offset': 0 }));
    // mockLogicalPlan.addLink(new OperatorLink('operator-1', 'operator-2'));
    // const body = this.getLogicalPlanRequest(mockLogicalPlan);

    const body = this.getLogicalPlanRequest(this.workflowModelService.logicalPlan);
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
