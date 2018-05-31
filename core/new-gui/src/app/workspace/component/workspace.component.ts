import { DragDropService } from './../service/drag-drop/drag-drop.service';
import { WorkflowUtilService } from './../service/workflow-graph/util/workflow-util.service';
import { WorkflowActionService } from './../service/workflow-graph/model/workflow-action.service';
import { Component, OnInit, AfterViewInit } from '@angular/core';

import { OperatorMetadataService } from '../service/operator-metadata/operator-metadata.service';
import { JointUIService } from '../service/joint-ui/joint-ui.service';
import { StubOperatorMetadataService } from '../service/operator-metadata/stub-operator-metadata.service';
import { SaveWorkflowService } from '../service/save-workflow/save-workflow.service';


@Component({
  selector: 'texera-workspace',
  templateUrl: './workspace.component.html',
  styleUrls: ['./workspace.component.scss'],
  providers: [
    // StubOperatorMetadataService can be used for debugging without start the backend server
    { provide: OperatorMetadataService, useClass: StubOperatorMetadataService },
    // OperatorMetadataService,

    JointUIService,
    WorkflowActionService,
    WorkflowUtilService,
    DragDropService,
    SaveWorkflowService,
  ]
})
export class WorkspaceComponent implements OnInit, AfterViewInit {

  constructor(
    private saveWorkflowService: SaveWorkflowService,
    private workflowActionService: WorkflowActionService,
  ) { }

  ngOnInit() {
  }

  ngAfterViewInit() {
    this.workflowActionService.workflowEditorRenderDone.next('angular afterViewInit called');
  }

}
