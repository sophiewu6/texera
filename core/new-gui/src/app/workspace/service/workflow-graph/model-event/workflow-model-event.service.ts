import { Injectable } from '@angular/core';
import { WorkflowModelService } from '../model/workflow-model.service';
import { Observable, Subject } from 'rxjs/Rx';
import '../../../common/rxjs-operators.ts';

@Injectable()
export class WorkflowModelEventService {

  public operatorAddedSubject = new Subject<{operator: OperatorPredicate, xOffset: number, yOffset: number}>();
  public operatorAddedObservable = this.operatorAddedSubject.asObservable();

  private operatorDeletedSubject = new Subject<{operatorID: string}>();
  public operatorDeletedObservable = this.operatorDeletedSubject.asObservable();

  private linkAddedSubject = new Subject<OperatorLink>();
  public linkAddedObservable = this.linkAddedSubject.asObservable().distinctUntilChanged();

  private linkDeletedSubject = new Subject<OperatorLink>();
  public linkDeletedObservable = this.linkDeletedSubject.asObservable();

  private linkChangedSubject = new Subject<OperatorLink>();
  public linkChangedObservable = this.linkChangedSubject.asObservable().distinctUntilChanged();

  public operatorPropertyChangedSubject = new Subject<{operatorID: string, newProperty: Object}>();

  constructor(
    private workflowModelService: WorkflowModelService
  ) {
    this.workflowModelService.uiGraph.on(
      'change:source change:target', (link: joint.dia.Link) => this.handleJointLinkChange(link));
    this.workflowModelService.uiGraph.on(
      'remove', (cell: joint.dia.Cell) => this.handleJointCellDelete(cell));

  }

  private handleJointLinkChange(link: joint.dia.Link): void {
    const sourceElement = link.getSourceElement();
    const targetElement = link.getTargetElement();
    // if the sourceElement and targetElement are both valid, then it means
    //   the link is acutally connected
    if (sourceElement && targetElement) {
      this.linkAddedSubject.next(
        {linkID: link.id.toString(), origin: sourceElement.id.toString(), destination: targetElement.id.toString()});
      return;
    }
    // if one of them is not valid, then it means that one side of the link
    //  is being dragged around, we need to delete the link if the link is previously connected
    // if (this.workflowModelService.logicalPlan.hasLink())

  }

  private handleJointCellDelete(cell: joint.dia.Cell): void {
    if (cell.isLink()) {
      // handle link deleted
      const link = <joint.dia.Link> cell;
      this.linkDeletedSubject.next();
    } else {
      // handle operator deleted
      const element = <joint.dia.Element> cell;
      this.operatorDeletedSubject.next({operatorID: element.id.toString()});
    }
  }



}
