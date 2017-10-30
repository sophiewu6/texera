import { Component, OnInit } from '@angular/core';

declare var $: JQueryStatic;

import * as _ from 'lodash';
import * as backbone from 'backbone';
import * as joint from 'jointjs';

import { CurrentWorkflowService } from '../../service/current-workflow/current-workflow.service';

@Component({
  selector: 'texera-workflow-editor',
  templateUrl: './workflow-editor.component.html',
  styleUrls: ['./workflow-editor.component.scss']
})
export class WorkflowEditorComponent implements OnInit {

  paper: joint.dia.Paper;

  constructor(private currentWorkflowService: CurrentWorkflowService) {

  }

  ngOnInit() {
    this.paper = new joint.dia.Paper({
      el: $('#workflow-holder'),
      width: 600,
      height: 200,
      model: this.currentWorkflowService.workflowUI,
      gridSize: 1
    });

    jQuery('#workflow-holder').droppable({
      drop: (event, ui) => this.createNewOperator(event, ui)
    });
  }

  private createNewOperator(event: Event, ui: JQueryUI.DroppableEventUIParam) {
    const operatorUIElement = new joint.shapes.basic.Rect({
      position: { x: ui.offset.left - this.paper.pageOffset().x, y: ui.offset.top - this.paper.pageOffset().y },
      size: { width: 100, height: 30 },
      attrs: { rect: { fill: 'blue' }, text: { text: 'my box', fill: 'white' } }
    });

    this.currentWorkflowService.workflowUI.addCell(operatorUIElement);
  }
}
