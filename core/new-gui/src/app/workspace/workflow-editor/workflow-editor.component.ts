import { Component, OnInit } from '@angular/core';
import * as joint from 'jointjs';


@Component({
  selector: 'texera-workflow-editor',
  templateUrl: './workflow-editor.component.html',
  styleUrls: ['./workflow-editor.component.scss']
})
export class WorkflowEditorComponent implements OnInit {

  constructor() {
    const graph = new joint.dia.Graph;

    const paper = new joint.dia.Paper({
            el: $('#workspace-holder'),
            width: 600,
            height: 200,
            model: graph,
            gridSize: 1
        });

    const rect = new joint.shapes.basic.Rect({
        position: { x: 100, y: 30 },
        size: { width: 100, height: 30 },
        attrs: { rect: { fill: 'blue' }, text: { text: 'my box', fill: 'white' } }
    });

    graph.addCells([rect]);

   }

  ngOnInit() {
  }

}
