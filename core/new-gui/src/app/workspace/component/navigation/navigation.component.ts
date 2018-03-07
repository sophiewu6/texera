import { Component, OnInit } from '@angular/core';

import { ExecuteWorkflowService } from '../../service/execute-workflow/execute-workflow.service';
import {MatProgressSpinnerModule} from '@angular/material/progress-spinner';
@Component({
  selector: 'texera-navigation',
  templateUrl: './navigation.component.html',
  styleUrls: ['./navigation.component.scss']
})
export class NavigationComponent implements OnInit {
  showSpinner = false;

  constructor(private executeWorkflowService: ExecuteWorkflowService) {
    executeWorkflowService.executeFinished$.subscribe(
      value => this.showSpinner = false,
      error => this.showSpinner = false
    )
  }

  ngOnInit() {
  }

  onClickRun() {
    this.showSpinner = true;
    this.executeWorkflowService.onExecuteWorkflowRequested();
  }

}
