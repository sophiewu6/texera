import { Component, OnInit, ViewChild } from '@angular/core';

import { NavigationComponent } from './navigation/navigation.component';
import { OperatorViewComponent } from './operator-view/operator-view.component';
import { WorkflowEditorComponent } from './workflow-editor/workflow-editor.component';
import { ResultViewComponent } from './result-view/result-view.component';
import { PropertyEditorComponent } from './property-editor/property-editor.component';

import { OperatorMetadataService } from '../service/operator-metadata/operator-metadata.service';
import { CurrentWorkflowService } from '../service/current-workflow/current-workflow.service';
import { WorkflowUiService } from '../service/current-workflow/workflow-ui.service';
import { OperatorDragDropService } from '../service/operator-drag-drop/operator-drag-drop.service';

@Component({
  selector: 'texera-workspace',
  templateUrl: './workspace.component.html',
  styleUrls: ['./workspace.component.scss'],
  providers: [
    OperatorMetadataService,
    CurrentWorkflowService,
    WorkflowUiService,
    OperatorDragDropService
  ]
})
export class WorkspaceComponent implements OnInit {

  constructor() { }

  ngOnInit() {
  }

}
