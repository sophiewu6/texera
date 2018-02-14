import { TestBed, inject } from '@angular/core/testing';

import { WorkflowDataEventService } from './workflow-data-event.service';

describe('WorkflowDataEventService', () => {
  beforeEach(() => {
    TestBed.configureTestingModule({
      providers: [WorkflowDataEventService]
    });
  });

  it('should be created', inject([WorkflowDataEventService], (service: WorkflowDataEventService) => {
    expect(service).toBeTruthy();
  }));
});
