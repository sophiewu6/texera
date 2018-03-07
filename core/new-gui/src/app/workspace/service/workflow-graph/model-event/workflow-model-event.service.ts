import { Injectable } from '@angular/core';
import { WorkflowJointGraphService } from '../model/workflow-joint-graph.service';
import { Observable, Subject } from 'rxjs/Rx';
import '../../../../common/rxjs-operators.ts';

@Injectable()
export class WorkflowModelEventService {

  public _operatorAddedSubject = new Subject<{operator: OperatorPredicate, xOffset: number, yOffset: number}>();
  public operatorAddedObservable = this._operatorAddedSubject.asObservable();

  public _operatorDeletedSubject = new Subject<{operatorID: string}>();
  public operatorDeletedObservable = this._operatorDeletedSubject.asObservable();

  public _linkAddedSubject = new Subject<OperatorLink>();
  public linkAddedObservable = this._linkAddedSubject.asObservable().distinctUntilChanged();

  public _linkDeletedSubject = new Subject<{linkID: string}>();
  public linkDeletedObservable = this._linkDeletedSubject.asObservable();

  public _linkChangedSubject = new Subject<OperatorLink>();
  public linkChangedObservable = this._linkChangedSubject.asObservable().distinctUntilChanged();

  public _operatorPropertyChangedSubject = new Subject<{operatorID: string, newProperty: Object}>();
  public operatorPropertyChangedObservable = this._operatorPropertyChangedSubject.asObservable().distinctUntilChanged();

  constructor(
    private workflowJointGraphService: WorkflowJointGraphService
  ) {
    this.workflowJointGraphService.uiGraph.on(
      'add', (cell: joint.dia.Cell) => this.handleJointCellAdd(cell));
    this.workflowJointGraphService.uiGraph.on(
      'remove', (cell: joint.dia.Cell) => this.handleJointCellDelete(cell));
    this.workflowJointGraphService.uiGraph.on(
      'change:source change:target', (link: joint.dia.Link) => this.handleJointLinkChange(link));
  }

  private static getOperatorLinkObject(link: joint.dia.Link): OperatorLink {
    const linkID = link.id.toString();
    let sourceOperator = null;
    let sourcePort = null;
    let targetOperator = null;
    let targetPort = null;

    if (link.getSourceElement()) {
      sourceOperator = link.getSourceElement().id.toString();
      sourcePort = link.get('source').port.toString();
    }

    if (link.getTargetElement()) {
      targetOperator = link.getTargetElement().id.toString();
      targetPort = link.get('target').port.toString();
    }

    return {linkID, sourceOperator, sourcePort, targetOperator, targetPort};
  }

  private handleJointCellAdd(cell: joint.dia.Cell): void {
    if (cell.isLink()) {
      this._linkAddedSubject.next(WorkflowModelEventService.getOperatorLinkObject(<joint.dia.Link> cell));
    }
  }

  private handleJointCellDelete(cell: joint.dia.Cell): void {
    if (cell.isLink()) {
      // handle link deleted
      const link = <joint.dia.Link> cell;
      this._linkDeletedSubject.next({linkID: link.id.toString()});
    } else {
      // handle operator deleted
      const element = <joint.dia.Element> cell;
      this._operatorDeletedSubject.next({operatorID: element.id.toString()});
    }
  }

  private handleJointLinkChange(link: joint.dia.Link): void {
    this._linkChangedSubject.next(WorkflowModelEventService.getOperatorLinkObject(link));
  }

}
