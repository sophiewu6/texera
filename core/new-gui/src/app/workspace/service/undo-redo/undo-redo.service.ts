import { Observable } from 'rxjs/Observable';
import { WorkflowActionService } from './../workflow-graph/model/workflow-action.service';
import { Injectable } from '@angular/core';

import { Stack } from 'typescript-collections';

// define the restricted methods that could change the stack
type restrictedStackMethods =
  'push' | 'add' | 'pop' | 'clear';

// define a type Omit that creates a type with certain methods/properties omitted from it
// http://ideasintosoftware.com/typescript-advanced-tricks/
type Diff<T extends string, U extends string> = ({ [P in T]: P } & { [P in U]: never } & { [x: string]: never })[T];
type Omit<T, K extends keyof T> = Pick<T, Diff<keyof T, K>>;
// TODO: after updating to Angular 6 (which uses Typescript > 2.8, this can be changed to the following)
// type Omit<T, K extends keyof T> = Pick<T, Exclude<keyof T, K>>;

type ReadonlyStack<T> = Omit<Stack<T>, restrictedStackMethods>

// the function type of an action that changes the workflow graph:
//  a function that takes no parameter and doesn't return anything
// type ActionFunction = (param?: any) => void;
// interface UndoRedoInfo extends Readonly<{
//   debugMessage: string;
//   undoAction: ActionFunction;
//   redoAction: ActionFunction;
// }> { };

/**
 * Typescript has this really serious issue of that
 *  a function has type signature of return void can return anything.
 * For example:
 *  let a: () => void; (a is a function that returns void)
 *  const b = () => 'returnValue' (b is a function that returns a string)
 *  a = b (no error, a function that returns a string can be assigned to a function that returns void)
 *
 * Typescript claims that it's the desired behavior, although it's not type safe
 * https://github.com/Microsoft/TypeScript/wiki/FAQ#why-are-functions-returning-non-void-assignable-to-function-returning-void
 * Countless issues have been opened to Typescript to fix it but Typescript won't accept it
 * https://github.com/Microsoft/TypeScript/issues/8581
 * https://github.com/Microsoft/TypeScript/issues/9603
 * https://github.com/Microsoft/TypeScript/issues/21674
 *
 * To workaround with that and get type checking back,
 *  we make a function explicitly returns a `Void` string to represent a function actually returns void
 *
 */
type VoidType = 'Void';
type ActionFunction = () => VoidType;
const VoidValue: VoidType = 'Void';

interface UndoActionInfo extends Readonly<{
  /**
   * a function that peforms the undo action and returns void
   */
  undoFunction: ActionFunction
  /**
   * a function that returns a another function
   *  the returned function performs the redo action and returns void
   */
  getRedoFunction: () => ActionFunction
}> { }


@Injectable()
export class UndoRedoService {

  private undoStack = new Stack<UndoActionInfo>();
  private redoStack = new Stack<ActionFunction>();

  private recordUndoRedoInfo: boolean = true;

  constructor(
    private workflowActionService: WorkflowActionService
  ) {

    Observable.merge(
      this.handleOperatorAdd(),
      // this.handleOperatorDelete(),
      // this.handleLinkAdd(),
      // this.handleLinkDelete(),
      // this.handleOperatorPropertyChange()
    )
      .filter(() => this.recordUndoRedoInfo)
      .subscribe(value => {
        console.log('action received');
        // console.log(value.debugMessage);
        this.undoStack.add(value);
        this.redoStack.clear();
      });

    console.log(this);
  }

  public getUndoStack(): ReadonlyStack<UndoActionInfo> {
    return this.undoStack;
  }

  public getRedoStack(): ReadonlyStack<ActionFunction> {
    return this.redoStack;
  }

  public undoAvailable(): boolean {
    return this.undoStack.size() > 0;
  }

  public undo(): void {
    const undoInfo = this.undoStack.pop();
    if (!undoInfo) {
      throw new Error(`undo stack is empty`);
    }
    // add the undoRedo info to the redo stack
    this.redoStack.add(undoInfo.getRedoFunction());

    // turn off subscribing to the events
    //  because they are caused by the undo action
    this.recordUndoRedoInfo = false;
    // perform the undo action
    undoInfo.undoFunction();
    // turn on subscribing to the events again
    this.recordUndoRedoInfo = true;
  }

  public redoAvailable(): boolean {
    return this.redoStack.size() > 0;
  }

  public redo(): void {
    const redoFunction = this.redoStack.pop();
    if (!redoFunction) {
      throw new Error(`redo stack is empty`);
    }
    // perform the redo action
    redoFunction();
  }

  private handleOperatorAdd(): Observable<UndoActionInfo> {
    return this.workflowActionService.getTexeraGraph().getOperatorAddStream()
      .map(value => {

        // a function to perform the undo operator - delete the operator and return Void
        const undoFunction = () => {
          this.workflowActionService.deleteOperator(value.operatorID);
          return VoidValue;
        }

        // a function returns the redo function, invoked when undo is called
        // first get a snapshot of the operator data before deleting it
        //  and use the snapshot data to the redo function
        const getRedoFunction = () => {
          // get the operator snapshot before proceeding to undo
          const operatorSnapshot = this.workflowActionService.getTexeraGraph().getOperator(value.operatorID);
          if (! operatorSnapshot) {
            throw new Error(`inconsistency: the operator ${value.operatorID} to be deleted is not in the graph`)
          }
          return () => {
            this.workflowActionService.addOperator(operatorSnapshot);
            return VoidValue;
          }
        }

        const undoActionInfo: UndoActionInfo = {
          undoFunction, getRedoFunction
        }
        return undoActionInfo;
      });
  }


  // private handleOperatorDelete(): Observable<UndoRedoInfo> {
  //   return this.workflowActionService.getTexeraGraph().getOperatorDeleteStream()
  //     .map(value => {
  //       const debugMessage = `event: delete operator ${value}`;
  //       const undoAction = () => this.workflowActionService.addOperator(value.deletedOperator);
  //       const redoAction = () => this.workflowActionService.deleteOperator(value.deletedOperator.operatorID);
  //       return { debugMessage, undoAction, redoAction };
  //     });
  // }

  // private handleLinkAdd(): Observable<UndoRedoInfo> {
  //   return this.workflowActionService.getTexeraGraph().getLinkAddStream()
  //     .map(value => {
  //       const debugMessage = `event: add link ${value}`;
  //       const undoAction = () => this.workflowActionService.deleteLinkWithID(value.linkID);
  //       const redoAction = () => this.workflowActionService.addLink(value);
  //       return { debugMessage, undoAction, redoAction };
  //     });
  // }

  // private handleLinkDelete(): Observable<UndoRedoInfo> {
  //   return this.workflowActionService.getTexeraGraph().getLinkDeleteStream()
  //     .map(value => {
  //       const debugMessage = `event: delete link ${value}`;
  //       const undoAction = () => this.workflowActionService.addLink(value.deletedLink);
  //       const redoAction = () => this.workflowActionService.deleteLinkWithID(value.deletedLink.linkID);
  //       return { debugMessage, undoAction, redoAction };
  //     });
  // }

  // private handleOperatorPropertyChange(): Observable<UndoRedoInfo> {
  //   return this.workflowActionService.getTexeraGraph().getOperatorPropertyChangeStream()
  //     .distinctUntilChanged()
  //     .throttleTime(500)
  //     .map(value => {
  //       const debugMessage = `event: change property ${value}`;
  //       // special case for set operator property:
  //       // in order to make property editor panel to show the changes, the operator needs to be highlighted first
  //       const undoAction = () => {
  //         this.workflowActionService.getJointGraphWrapper().highlightOperator(value.operator.operatorID);
  //         this.workflowActionService.setOperatorProperty(value.operator.operatorID, value.oldProperty);
  //       }
  //       const redoAction = () => {
  //         this.workflowActionService.getJointGraphWrapper().highlightOperator(value.operator.operatorID);
  //         this.workflowActionService.setOperatorProperty(
  //           value.operator.operatorID, value.operator.operatorProperties);
  //       }
  //       return { debugMessage, undoAction, redoAction };
  //     });
  // }



}
