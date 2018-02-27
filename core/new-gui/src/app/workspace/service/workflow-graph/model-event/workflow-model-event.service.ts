import { Injectable } from '@angular/core';
import { WorkflowModelService } from '../model/workflow-model.service';
import { Observable, Subject } from 'rxjs/Rx';
import '../../../common/rxjs-operators.ts';

@Injectable()
export class WorkflowModelEventService {

  public operatorAddedSubject = new Subject<any>();

  public operatorDeletedSubject = new Subject<any>();

  public linkAddedSubject = new Subject<any>();

  public linkDeletedSubject = new Subject<any>();

  public linkChangedSubject = new Subject<any>();

  public operatorPropertyChangedSubject = new Subject<{operatorID: string, newProperty: Object}>();



  constructor(
    private workflowModelService: WorkflowModelService
  ) {
    this.workflowModelService.uiGraph.on('add', (cell: joint.dia.Cell) => this.handleJointModelAdd(cell));

  }

  private handleJointModelAdd(cell: joint.dia.Cell) {
    if (cell.isLink()) {
      const link = <joint.dia.Link>cell;
      this.linkAddedSubject.next(link.attributes);
    }

  }



}
