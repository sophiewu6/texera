import { Injectable } from '@angular/core';
import { WorkflowModelService } from '../model/workflow-model.service';
import { Observable, Subject } from 'rxjs/Rx';
import '../../../common/rxjs-operators.ts';

@Injectable()
export class WorkflowModelEventService {

  public operatorAddedSubject = new Subject<{operator: OperatorPredicate, xOffset: number, yOffset: number}>();

  public operatorDeletedSubject = new Subject<any>();

  public linkAddedSubject = new Subject<{operatorLink: OperatorLink}>();

  public linkDeletedSubject = new Subject<any>();

  public linkChangedSubject = new Subject<any>();

  public operatorPropertyChangedSubject = new Subject<{operatorID: string, newProperty: Object}>();



  constructor(
    private workflowModelService: WorkflowModelService
  ) {
    this.workflowModelService.uiGraph.on('add', (cell: joint.dia.Cell) => this.handleJointModelAdd(cell));
    this.workflowModelService.uiGraph.on('change', (cell: joint.dia.Cell) => this.handleJointCellChange(cell));
  }

  private handleJointCellChange(cell: joint.dia.Cell) {
    if (cell.isLink) {
      const link = <joint.dia.Link>cell;
      const sourceElement = link.getSourceElement();
      const targetElement = link.getTargetElement();
      if (sourceElement){  }

    }


  }

  /*
    Only handle to the add link event and push to the link added subject.
    Operator added subject will be direclty pushed by the action, not here.
  */
  private handleJointModelAdd(cell: joint.dia.Cell) {
    if (cell.isLink()) {
      const link = <joint.dia.Link>cell;

      this.linkAddedSubject.next({});
      this.linkAddedSubject.next(link.attributes);
    }
  }



}
