import { TestBed, inject } from '@angular/core/testing';

import { WorkflowModelEventService } from './workflow-model-event.service';

describe('WorkflowModelEventService', () => {
  beforeEach(() => {
    TestBed.configureTestingModule({
      providers: [WorkflowModelEventService]
    });
  });

  it('should be created', inject([WorkflowModelEventService], (service: WorkflowModelEventService) => {
    expect(service).toBeTruthy();
  }));
});
