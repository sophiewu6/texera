import { TestBed, inject } from '@angular/core/testing';

import { WorkflowUiControlService } from './workflow-ui-control.service';

describe('WorkflowUiControlService', () => {
  beforeEach(() => {
    TestBed.configureTestingModule({
      providers: [WorkflowUiControlService]
    });
  });

  it('should be created', inject([WorkflowUiControlService], (service: WorkflowUiControlService) => {
    expect(service).toBeTruthy();
  }));
});
