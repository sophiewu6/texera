import { TestBed, inject } from '@angular/core/testing';

import { WorkflowDataControlService } from './workflow-data-control.service';

describe('WorkflowDataControlService', () => {
  beforeEach(() => {
    TestBed.configureTestingModule({
      providers: [WorkflowDataControlService]
    });
  });

  it('should be created', inject([WorkflowDataControlService], (service: WorkflowDataControlService) => {
    expect(service).toBeTruthy();
  }));
});
