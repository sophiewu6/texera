import { TestBed, inject } from '@angular/core/testing';

import { WorkflowDataChangeService } from './workflow-data-change.service';

describe('WorkflowDataChangeService', () => {
  beforeEach(() => {
    TestBed.configureTestingModule({
      providers: [WorkflowDataChangeService]
    });
  });

  it('should be created', inject([WorkflowDataChangeService], (service: WorkflowDataChangeService) => {
    expect(service).toBeTruthy();
  }));
});
