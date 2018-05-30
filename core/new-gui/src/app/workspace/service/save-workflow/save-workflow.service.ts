import { JointUIService } from './../joint-ui/joint-ui.service';
import { HttpClient } from '@angular/common/http';
import { JointGraphWrapper } from './../workflow-graph/model/joint-graph-wrapper';
import { OperatorLink, OperatorPredicate, Point } from './../../types/workflow-common.interface';
import { WorkflowActionService } from './../workflow-graph/model/workflow-action.service';
import { Injectable } from '@angular/core';

type OperatorWithPoint = OperatorPredicate & { point: Point };

interface SaveableWorkflow {
  operatorsWithPoints: OperatorWithPoint[];
  links: OperatorLink[];
}

interface SaveWorkflowResponse {
  status: 'success' | 'fail';
  message: string;
}

interface LoadWorkflowResponse {
  status: 'success' | 'fail';
  message: string;
  savedWorkflow: SaveableWorkflow;
}

@Injectable()
export class SaveWorkflowService {

  constructor(
    private workflowActionService: WorkflowActionService,
    private jointUIService: JointUIService,
    private httpClient: HttpClient
  ) { }

  public saveCurrentWorkflow(): void {
    const saveableWorkflow = this.getSaveableWorkflow();
    this.httpClient.post('', saveableWorkflow).subscribe();
  }

  public loadSavedWorkflow(): void {
    this.workflowActionService.clearAll();

    this.httpClient.get<LoadWorkflowResponse>('').subscribe(
      (data) => {
        if (data.status === 'success') {
          data.savedWorkflow.operatorsWithPoints.forEach(
            opWithPoint => {
              const {point, ...operator} = opWithPoint;
              this.workflowActionService.addOperator(
                operator, this.jointUIService.getJointOperatorElement(operator, point)
              );
            }
          );
          data.savedWorkflow.links.forEach(
            link => {
              this.workflowActionService.addLink(link);
            }
          );
        } else {
          // TODO
        }
      }
    );
  }

  private getSaveableWorkflow(): SaveableWorkflow {
    const texeraGraph = this.workflowActionService.getTexeraGraph();
    const jointGraphWrapper = this.workflowActionService.getJointGraphWrapper();

    const operators = texeraGraph.getOperators();
    const links = texeraGraph.getLinks();

    const operatorsWithPoints = operators.map(op => ({
      ...op,
      point: jointGraphWrapper.getOperatorElementPosition(op.operatorID)
    }));

    const saveableWorkflow = { operatorsWithPoints, links };

    return saveableWorkflow;
  }

}
