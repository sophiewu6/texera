import { Component, OnInit } from '@angular/core';

declare var $: JQueryStatic;

import * as _ from 'lodash';
import * as backbone from 'backbone';
import * as joint from 'jointjs';

import { OperatorDragDropService } from '../../service/operator-drag-drop/operator-drag-drop.service';
import { WorkflowModelService } from '../../service/workflow-graph/workflow-model.service';
import { WorkflowUIChangeService } from '../../service/workflow-graph/workflow-ui-change.service';


@Component({
  selector: 'texera-workflow-editor',
  templateUrl: './workflow-editor.component.html',
  styleUrls: ['./workflow-editor.component.scss']
})
export class WorkflowEditorComponent implements OnInit {

  // the id in HTML must also be changed whenever this is changed
  readonly WORKFLOW_EDITOR_ID = 'texera-workflow-holder';
  private paper: joint.dia.Paper;

  constructor(
    private workflowModelSerivce: WorkflowModelService,
    private workflowUIChangeService: WorkflowUIChangeService,
    private operatorDragDropService: OperatorDragDropService) {

  }

  ngOnInit() {
    this.paper = new joint.dia.Paper({
      el: $('#' + this.WORKFLOW_EDITOR_ID),
      width: 600,
      height: 200,
      model: this.workflowModelSerivce.uiGraph,
      gridSize: 1,
      snapLinks: true,
      linkPinning: false,
      validateConnection: function (sourceView, sourceMagnet, targetView, targetMagnet) {

        // user cannot draw connection starting from the input port (left side)
        if (sourceMagnet && sourceMagnet.getAttribute('port-group') === 'in') {return false; }

        // user cannot connect to the output port (right side)
        if (targetMagnet && targetMagnet.getAttribute('port-group') === 'out') {return false; }

        return sourceView.id !== targetView.id;
        // return sourceMagnet !== targetMagnet;
      }
    });

    this.operatorDragDropService.registerDrop(this.WORKFLOW_EDITOR_ID);

    this.workflowModelSerivce.registerWorkflowPaper(this.paper);

    this.paper.on('cell:pointerdown', (cellView, evt, x, y) => {
      console.log("Cell pointer down occurrs");
      this.workflowUIChangeService.selectOperator(cellView.model.id);
    });


    // test delete action
    this.paper.on('element:delete', (cellView, evt, x, y) => {
      evt.stopPropagation();
      // delete the operator in the workflow data
      this.workflowUIChangeService.deleteOperator(cellView.model.id);

      // delete the operator on the cellView
      cellView.model.remove();
    });
  }


}
