import { Component, OnInit } from '@angular/core';

import { ExecuteWorkflowService } from '../../service/execute-workflow/execute-workflow.service';
import {MatProgressSpinnerModule} from '@angular/material/progress-spinner';
@Component({
  selector: 'texera-navigation',
  templateUrl: './navigation.component.html',
  styleUrls: ['./navigation.component.scss']
})
export class NavigationComponent implements OnInit {
  showSpinner: boolean = false;

  constructor(private executeWorkflowService: ExecuteWorkflowService) { }

  ngOnInit() {
  }

  onClickRun() {
    this.executeWorkflowService.onExecuteWorkflowRequested();
    this.showSpinner=!this.showSpinner;
  }

}
