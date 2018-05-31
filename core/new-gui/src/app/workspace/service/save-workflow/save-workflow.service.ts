import { Observable } from 'rxjs/Observable';
import { JointUIService } from './../joint-ui/joint-ui.service';
import { JointGraphWrapper } from './../workflow-graph/model/joint-graph-wrapper';
import { OperatorLink, OperatorPredicate, Point } from './../../types/workflow-common.interface';
import { WorkflowActionService } from './../workflow-graph/model/workflow-action.service';
import { Injectable } from '@angular/core';
import { SessionStorageService } from 'ngx-webstorage';

type OperatorWithPoint = OperatorPredicate & { point: Point };

export interface SaveableWorkflow {
  operatorsWithPoints: OperatorWithPoint[];
  links: OperatorLink[];
}

@Injectable()
export class SaveWorkflowService {

  constructor(
    private workflowActionService: WorkflowActionService,
    private jointUIService: JointUIService,
    private sessionStorageService: SessionStorageService
  ) {
    const texeraGraph = this.workflowActionService.getTexeraGraph();
    const jointGraphWrapper = this.workflowActionService.getJointGraphWrapper();

    Observable.merge(
      texeraGraph.getOperatorAddStream(),
      texeraGraph.getOperatorDeleteStream(),
      texeraGraph.getLinkAddStream(),
      texeraGraph.getLinkDeleteStream(),
      texeraGraph.getOperatorPropertyChangeStream().auditTime(200),
      jointGraphWrapper.getOperatorPositionChangeStream().auditTime(500)
    ).auditTime(1000).subscribe(
      () => this.autoSaveCurrentWorkflow()
    );

  }

  public autoSaveCurrentWorkflow(): void {
    console.log('auto save in progress...');
    const saveableWorkflow = this.transformSaveableWorkflow();
    this.sessionStorageService.store('texera-saved-workflow', JSON.stringify(saveableWorkflow));
  }

  private transformSaveableWorkflow(): SaveableWorkflow {
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
