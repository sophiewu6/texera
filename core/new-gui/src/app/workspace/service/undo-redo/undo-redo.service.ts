import { Observable } from 'rxjs/Observable';
import { WorkflowActionService } from './../workflow-graph/model/workflow-action.service';
import { Injectable } from '@angular/core';

import { Stack } from 'typescript-collections';

// the function type of an action that changes the workflow graph:
//  a function that takes no parameter and doesn't return anything
type ActionFunction = () => void;
interface UndoRedoInfo {
  debugMessage: string;
  undoAction: ActionFunction;
  redoAction: ActionFunction;
}

@Injectable()
export class UndoRedoService {

  private undoStack = new Stack<UndoRedoInfo>();
  private redoStack = new Stack<UndoRedoInfo>();

  private recordUndoRedoInfo: boolean = true;

  constructor(
    private workflowActionService: WorkflowActionService
  ) {

    Observable.merge(
      this.handleOperatorAdd(),
      this.handleOperatorDelete(),
      this.handleLinkAdd(),
      this.handleLinkDelete(),
      this.handleOperatorPropertyChange()
    )
    .filter(() => this.recordUndoRedoInfo)
    .subscribe(value => {
      console.log('action received');
      console.log(value.debugMessage);
      this.undoStack.add(value);
      this.redoStack.clear();
    });

    console.log(this);
  }

  public undoAvailable(): boolean {
    return this.undoStack.size() > 0;
  }

  public undo(): void {
    console.log('trying to undo');
    const action = this.undoStack.pop();
    if (! action) {
      throw new Error(`undo stack is empty`);
    }
    console.log(action);
    console.log(this.undoStack.size());
    // add the undoRedo info to the redo stack
    this.redoStack.add(action);

    // turn off subscribing to the events
    //  because they are caused by the undo action
    this.recordUndoRedoInfo = false;
    // perform the undo action
    action.undoAction();
    // turn on subscribing to the events again
    this.recordUndoRedoInfo = true;
    console.log('undo finished');
  }

  public redoAvailable(): boolean {
    return this.redoStack.size() > 0;
  }

  public redo(): void {
    const action = this.redoStack.pop();
    if (! action) {
      throw new Error(`redo stack is empty`);
    }
    // perform the redo action
    action.redoAction();
  }

  private handleOperatorAdd(): Observable<UndoRedoInfo> {
    return this.workflowActionService.getTexeraGraph().getOperatorAddStream()
      .map(value => {
        const debugMessage = `event: add operator ${value}`;
        const undoAction = () => this.workflowActionService.deleteOperator(value.operatorID);
        const redoAction = () => this.workflowActionService.addOperator(value);
        return {debugMessage, undoAction, redoAction};
      });
  }

  private handleOperatorDelete(): Observable<UndoRedoInfo> {
    return this.workflowActionService.getTexeraGraph().getOperatorDeleteStream()
      .map(value => {
        const debugMessage = `event: delete operator ${value}`;
        const undoAction = () => this.workflowActionService.addOperator(value.deletedOperator);
        const redoAction = () => this.workflowActionService.deleteOperator(value.deletedOperator.operatorID);
        return {debugMessage, undoAction, redoAction};
      });
  }

  private handleLinkAdd(): Observable<UndoRedoInfo> {
    return this.workflowActionService.getTexeraGraph().getLinkAddStream()
      .map(value => {
        const debugMessage = `event: add link ${value}`;
        const undoAction = () => this.workflowActionService.deleteLinkWithID(value.linkID);
        const redoAction = () => this.workflowActionService.addLink(value);
        return {debugMessage, undoAction, redoAction};
      });
  }

  private handleLinkDelete(): Observable<UndoRedoInfo> {
    return this.workflowActionService.getTexeraGraph().getLinkDeleteStream()
      .map(value => {
        const debugMessage = `event: delete link ${value}`;
        const undoAction = () => this.workflowActionService.addLink(value.deletedLink);
        const redoAction = () => this.workflowActionService.deleteLinkWithID(value.deletedLink.linkID);
        return {debugMessage, undoAction, redoAction};
      });
  }

  private handleOperatorPropertyChange(): Observable<UndoRedoInfo> {
    return this.workflowActionService.getTexeraGraph().getOperatorPropertyChangeStream()
      .distinctUntilChanged()
      .throttleTime(500)
      .map(value => {
        const debugMessage = `event: change property ${value}`;
        const undoAction = () => this.workflowActionService.setOperatorProperty(value.operator.operatorID, value.oldProperty);
        const redoAction = () => this.workflowActionService.setOperatorProperty(
          value.operator.operatorID, value.operator.operatorProperties);
        return {debugMessage, undoAction, redoAction};
      });
  }



}