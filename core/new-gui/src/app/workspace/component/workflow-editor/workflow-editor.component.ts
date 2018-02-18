import { Component, OnInit } from '@angular/core';
import { Observable, Subject } from 'rxjs/Rx';
import '../../../common/rxjs-operators.ts';

declare var $: JQueryStatic;

import * as _ from 'lodash';
import * as backbone from 'backbone';
import * as joint from 'jointjs';

import { OperatorDragDropService } from '../../service/operator-drag-drop/operator-drag-drop.service';
import { WorkflowModelService } from '../../service/workflow-graph/model/workflow-model.service';
import { WorkflowUIChangeService } from '../../service/workflow-graph/workflow-ui-change.service';


@Component({
  selector: 'texera-workflow-editor',
  templateUrl: './workflow-editor.component.html',
  styleUrls: ['./workflow-editor.component.scss']
})
export class WorkflowEditorComponent implements OnInit {

  // the DOM element ID of the main editor
  // the id in the corresponding html file must also be changed whenever this is changed
  readonly WORKFLOW_EDITOR_ID = 'texera-workflow-holder';
  private paper: joint.dia.Paper = this.createJointjsPaper();

  // private elementSelectedInEditor$ = Observable.fromEventPattern(
  //   this.paper.on(cell:pointerdown)
  // )

  public operatorSelectedInEditor = new Subject<string>();
  public operatorDeletedInEditor = new Subject<string>();

  public linkSelectedInEditor = new Subject<string>();
  public linkDeletedInEditor = new Subject<string>();


  constructor(
    private workflowModelSerivce: WorkflowModelService,
    private workflowUIChangeService: WorkflowUIChangeService,
    private operatorDragDropService: OperatorDragDropService) {
  }

  ngOnInit() {
    // create the jointJS paper with custom configurations
    // this.paper = this.createJointjsPaper();

    // register the DOM element ID to drag and drop service
    this.operatorDragDropService.registerDrop(this.WORKFLOW_EDITOR_ID);
    // register the jointJS paper object to the workflow model
    this.workflowModelSerivce.registerWorkflowPaper(this.paper);

    // handle jointJS events
    this.paper.on('cell:pointerdown', this.handleCellPointDown);
    this.paper.on('element:delete', this.handleElementDelete);
  }

  private createJointjsPaper(): joint.dia.Paper {
    const paper = new joint.dia.Paper({
      el: $('#' + this.WORKFLOW_EDITOR_ID),
      width: 600,
      height: 200,
      model: this.workflowModelSerivce.uiGraph,
      gridSize: 1,
      snapLinks: true,
      linkPinning: false,
      validateConnection: this.validateOperatorConnection
    });

    return paper;
  }

  private validateOperatorConnection(sourceView: joint.dia.CellView, sourceMagnet: SVGElement,
    targetView: joint.dia.CellView, targetMagnet: SVGElement): boolean {
    // user cannot draw connection starting from the input port (left side)
    if (sourceMagnet && sourceMagnet.getAttribute('port-group') === 'in') { return false; }

    // user cannot connect to the output port (right side)
    if (targetMagnet && targetMagnet.getAttribute('port-group') === 'out') { return false; }

    return sourceView.id !== targetView.id;
  }

  private handleCellPointDown(cellView: joint.dia.CellView, evt: Event, x: number, y: number) {

  }

  private handleElementDelete(cellView: joint.dia.CellView, evt: Event, x: number, y: number) {
    // evt.stopPropagation();
    // // delete the operator in the workflow data
    // this.workflowUIChangeService.deleteOperator(cellView.model.id);

    // // delete the operator on the cellView
    // cellView.model.remove();
  }


}
