import { Component, OnInit, AfterViewInit } from '@angular/core';
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
export class WorkflowEditorComponent implements AfterViewInit {

  // the DOM element ID of the main editor
  // the id in the corresponding html file must also be changed whenever this is changed
  readonly WORKFLOW_EDITOR_ID = 'texera-workflow-holder';
  // the jointJS
  private paper: joint.dia.Paper = this.createJointjsPaper();


  /*
    The Workflow Editor Component (the main editor) wraps the JointJS event as Obserables.
    It listens to the events on JointJS paper and push the events to the corresponding Subject.
    Subjects are private because only this component can push events to the subject, and
      observables are public so that outside modules can subscribe

    Currently the following events are provided as Observable:
      - operator selected in editor
      - link selected in editor
      - operator deleted in editor
      - link deleted in editor
      - link added in editor
  */

  private operatorSelectedInEditor = new Subject<string>();
  public operatorSelectedInEditor$ = this.operatorSelectedInEditor.asObservable();

  private linkSelectedInEditor = new Subject<string>();
  public linkSelectedInEditor$ = this.linkSelectedInEditor.asObservable();

  private operatorDeletedInEditor = new Subject<string>();
  public operatorDeletedInEditor$ = this.operatorDeletedInEditor.asObservable();

  private linkDeletedInEditor = new Subject<string>();
  public linkDeletedInEditor$ = this.linkDeletedInEditor$.asObservable();

  private linkAddedInEditor = new Subject<string>();
  public linkAddedInEditor$ = this.linkAddedInEditor.asObservable();


  constructor(
    private workflowModelSerivce: WorkflowModelService,
    private workflowUIChangeService: WorkflowUIChangeService,
    private operatorDragDropService: OperatorDragDropService) {
  }

  ngAfterViewInit() {
    // create the jointJS paper with custom configurations
    this.paper = this.createJointjsPaper();

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
    // in jointJS, a cell can either be an operator or a link
    if (this.workflowModelSerivce.logicalPlan.hasOperator(cellView.id)) {
      // an operator cell is pointed down
      // push the operator ID to the subject
      this.operatorSelectedInEditor.next(cellView.id);
    } else {
      // a link cell is pointed down
      // get the source and destination id first
      // in jointJS, if an operator is removed, the links will be automatically removed as well
      // therefore each existing link must have source and destination port
    }
  }

  private handleElementDelete(cellView: joint.dia.CellView, evt: Event, x: number, y: number) {
    // evt.stopPropagation();
    // // delete the operator in the workflow data
    // this.workflowUIChangeService.deleteOperator(cellView.model.id);

    // // delete the operator on the cellView
    // cellView.model.remove();
  }


}
