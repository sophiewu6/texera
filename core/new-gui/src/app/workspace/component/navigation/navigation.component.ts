import { Component, OnInit } from '@angular/core';

import { ExecuteWorkflowService } from '../../service/execute-workflow/execute-workflow.service';

@Component({
  selector: 'texera-navigation',
  templateUrl: './navigation.component.html',
  styleUrls: ['./navigation.component.scss']
})
export class NavigationComponent implements OnInit {

  constructor(private executeWorkflowService: ExecuteWorkflowService) { }

  ngOnInit() {
  }

  onClickRun() {
    this.executeWorkflowService.executeWorkflow();
  }

}
