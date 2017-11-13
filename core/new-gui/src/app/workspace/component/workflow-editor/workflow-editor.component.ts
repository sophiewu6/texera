import { Component, OnInit } from '@angular/core';

declare var $: JQueryStatic;

import * as _ from 'lodash';
import * as backbone from 'backbone';
import * as joint from 'jointjs';

import { WorkflowDataService } from '../../service/current-workflow/workflow-data.service';
import { OperatorDragDropService } from '../../service/operator-drag-drop/operator-drag-drop.service';

@Component({
  selector: 'texera-workflow-editor',
  templateUrl: './workflow-editor.component.html',
  styleUrls: ['./workflow-editor.component.scss']
})
export class WorkflowEditorComponent implements OnInit {

  paper: joint.dia.Paper;

  constructor(private workflowDataService: WorkflowDataService, private operatorDragDropService: OperatorDragDropService) {

  }

  ngOnInit() {
    this.paper = new joint.dia.Paper({
      el: $('#workflow-holder'),
      width: 600,
      height: 200,
      model: this.workflowDataService.workflowUI,
      gridSize: 1
    });

    jQuery('#workflow-holder').droppable({
      drop: (event, ui) => this.operatorDragDropService.onOperatorDrop(event, ui)
    });

    this.workflowDataService.registerWorkflowPaper(this.paper);
  }

}
