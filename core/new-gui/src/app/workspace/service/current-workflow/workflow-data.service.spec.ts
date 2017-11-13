import { TestBed, inject } from '@angular/core/testing';

import { WorkflowDataService } from './workflow-data.service';

describe('WorkflowDataService', () => {
  beforeEach(() => {
    TestBed.configureTestingModule({
      providers: [WorkflowDataService]
    });
  });

  it('should be created', inject([WorkflowDataService], (service: WorkflowDataService) => {
    expect(service).toBeTruthy();
  }));
});
