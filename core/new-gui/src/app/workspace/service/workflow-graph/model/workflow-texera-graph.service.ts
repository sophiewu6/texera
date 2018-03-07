import { Injectable } from '@angular/core';

import { WorkflowGraph } from '../../../model/workflow-graph';
import { WorkflowModelEventService } from '../model-event/workflow-model-event.service';


@Injectable()
export class WorkflowTexeraGraphService {

  private texeraGraph = new WorkflowGraph([], []);

  public texeraWorkflowGraph: WorkflowGraphReadonly = this.texeraGraph;

  constructor(private workflowModelEventService: WorkflowModelEventService) {
    this.bindModelEvents();
  }

  private bindModelEvents(): void {
    this.workflowModelEventService.operatorAddedObservable.subscribe(
      data => this.texeraGraph.addOperator(data.operator)
    );

    this.workflowModelEventService.operatorDeletedObservable.subscribe(
      data => this.texeraGraph.deleteOperator(data.operatorID)
    );

    this.workflowModelEventService.linkAddedObservable.subscribe(
      data => this.texeraGraph.addLink(data)
    );

    this.workflowModelEventService.linkDeletedObservable.subscribe(
      data => this.texeraGraph.deleteLink(data.linkID)
    );

    this.workflowModelEventService.linkChangedObservable.distinctUntilChanged().subscribe(
      data => this.texeraGraph.changeLink(data)
    );

    this.workflowModelEventService.operatorPropertyChangedSubject.distinctUntilChanged().subscribe(
      data => this.texeraGraph.changeOperatorProperty(data.operatorID, data.newProperty)
    );
  }

}
