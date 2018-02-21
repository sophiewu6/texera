import { Component, OnInit, AfterViewInit } from '@angular/core';
import { Observable, Subject } from 'rxjs/Rx';
import '../../../common/rxjs-operators.ts';

declare var $: JQueryStatic;
import * as joint from 'jointjs';

import { OperatorDragDropService } from '../../service/operator-drag-drop/operator-drag-drop.service';
import { WorkflowModelService } from '../../service/workflow-graph/model/workflow-model.service';
import { WorkflowViewEventService } from '../../service/workflow-graph/view-event/workflow-view-event.service';


/**
 * WorkflowEditorComponent is the componenet for the main workflow editor part of the UI.
 *
 * This componenet is binded to a JointJS paper. JointJS will handle the operations of the main workflow.
 * The JointJS UI events are wrapped into observables and exposed to other components / services.
 *
 * See JointJS documentation for the list of events that can be captured on the JointJS paper view.
 * https://resources.jointjs.com/docs/jointjs/v2.0/joint.html#dia.Paper.events
 *
*/
@Component({
  selector: 'texera-workflow-editor',
  templateUrl: './workflow-editor.component.html',
  styleUrls: ['./workflow-editor.component.scss']
})
export class WorkflowEditorComponent implements AfterViewInit {

  // the DOM element ID of the main editor
  // the id in the corresponding html file must also be changed whenever this is changed
  public readonly WORKFLOW_EDITOR_ID: string = 'texera-workflow-holder';
  // the jointJS paper
  private paper: joint.dia.Paper = this.createJointjsPaper();

  constructor(
    private workflowModelSerivce: WorkflowModelService,
    private workflowViewEventService: WorkflowViewEventService,
    private operatorDragDropService: OperatorDragDropService) {
  }

  ngAfterViewInit() {
    // create the jointJS paper with custom configurations
    this.paper = this.createJointjsPaper();

    // register the paper to the model
    this.workflowModelSerivce.registerWorkflowPaper(this.paper);

    // register the DOM element ID of this compoenent to drag and drop service
    this.operatorDragDropService.registerWorkflowEditorDrop(this.WORKFLOW_EDITOR_ID);

    // handle jointJS events
    this.bindJointPaperEvents();
  }

  /**
   * Creates a JointJS Paper object, which is the view that is responsible for
   *  rendering the workflow cells and handle UI events.
   *
   * JointJS documentation about paper: https://resources.jointjs.com/docs/jointjs/v2.0/joint.html#dia.Paper
   */
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

  /**
   * This function is provided to JointJS to disable some invalid connections on the UI.
   * If the connection is invalid, users are not able to connect the links on the UI.
   *
   * https://resources.jointjs.com/docs/jointjs/v2.0/joint.html#dia.Paper.prototype.options.validateConnection
   *
   * @param sourceView
   * @param sourceMagnet
   * @param targetView
   * @param targetMagnet
   */
  private validateOperatorConnection(sourceView: joint.dia.CellView, sourceMagnet: SVGElement,
    targetView: joint.dia.CellView, targetMagnet: SVGElement): boolean {
    // user cannot draw connection starting from the input port (left side)
    if (sourceMagnet && sourceMagnet.getAttribute('port-group') === 'in') { return false; }

    // user cannot connect to the output port (right side)
    if (targetMagnet && targetMagnet.getAttribute('port-group') === 'out') { return false; }

    return sourceView.id !== targetView.id;
  }

  /**
   * bind the JointJS Paper events to functions that push the event to the corresponding Subject
  */
  private bindJointPaperEvents(): void {
    this.paper.on('cell:pointerdown', this.handleCellPointerDown);

    this.paper.on('element:delete', this.handleElementDelete);

    this.paper.on('blank:pointerdown', (evt: Event, x: number, y: number) => {
      this.workflowViewEventService.pointerDownOnBlankInEditor.next({ 'event': evt, 'x': x, 'y': y });
    });

    this.paper.on('link:connect', (linkView, evt, elementViewConnected, magnet, arrowhead) => {
      this.workflowViewEventService.linkConnectedInEditor.next({
        linkView, evt, elementViewConnected, magnet, arrowhead
      });
    });

    this.paper.on('link:disconnect', (linkView, evt, elementViewDisconnected, magnet, arrowhead) => {
      this.workflowViewEventService.linkDisconnectedInEditor.next({
        linkView, evt, elementViewDisconnected, magnet, arrowhead
      });
    });
  }


  /**
   * Handle JointJS paper cell:pointerdown event.
   * Check if the cell is an operator, and push the operator ID of the clicked operator to the corresponding subject
   */
  private handleCellPointerDown(cellView: joint.dia.CellView, evt: Event, x: number, y: number) {
    // in jointJS, a cell can either be an operator or a link
    // cell.id is the same as operatorID of the operator
    if (this.workflowModelSerivce.logicalPlan.hasOperator(cellView.id)) {
      // an operator cell is pointed down
      // push the operator ID to the subject
      this.workflowViewEventService.operatorSelectedInEditor.next(cellView.id);
    }
  }

  /**
   * Handle JointJS paper element:delete event.
   *
   * element:delete event is not in origin JointJS paper event API.
   * This event is made possible by setting the
   *  '.delete-button' event to 'element:delete' in OperatorUIElementService class.
   *
   */
  private handleElementDelete(cellView: joint.dia.CellView, evt: Event, x: number, y: number) {
    this.workflowViewEventService.deleteOperatorClickedInEditor.next(cellView.id);
  }

}
