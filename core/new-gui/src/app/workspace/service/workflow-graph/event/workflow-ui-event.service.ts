import { Injectable } from '@angular/core';
import { Observable } from 'rxjs/Rx';
import '../../../common/rxjs-operators.ts';
import { WorkflowModelService } from '../model/workflow-model.service';
import { WorkflowUiControlService } from '../controller/workflow-ui-control.service';

@Injectable()
export class WorkflowUiEventService {

  constructor(private workflowModelService: WorkflowModelService, private workflowUiControlService: WorkflowUiControlService) {}

  // add operator event will only come from external control
  private externalAddOperator$ = this.workflowUiControlService._addOperatorSubject.asObservable();
  public uiAddOperator$ = this.externalAddOperator$;

  private jointjsDeleteCallback = Observable.bindCallback<string, any>(
    (arg: string, callback: Function) => this.workflowModelService.uiGraph.on(arg, callback, null));

  private externalDeleteOperator$ = this.workflowUiControlService._deleteOperatorSubject.asObservable();

  private jointjsDeleteOperator$ = this.jointjsDeleteCallback('remove')
    .filter(cell => cell.isElement())
    .filter(cell => this.workflowModelService.logicalPlan.hasOperator(cell.id))
    .map(cell => this.workflowModelService.logicalPlan.getOperator(cell.id));

  private uiDeleteOperator$ = Observable.merge(this.externalDeleteOperator$, this.jointjsDeleteOperator$);


  private jointjsDeleteLink$ = this.jointjsDeleteCallback('remove')
    .filter((cell: joint.dia.Cell) => cell.isLink())
    .map((cell: joint.dia.Link): OperatorLink => ({
      'origin': cell.getSourceElement().id.toString(),
      'destination': cell.getTargetElement().id.toString()}))
    .filter(operatorLink => this.workflowModelService.logicalPlan.hasLink(operatorLink));

  private uiDeleteLink$ = Observable.merge(this.jointjsDeleteLink$);


}
