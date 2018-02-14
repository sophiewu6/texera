import { TestBed, inject } from '@angular/core/testing';

import { WorkflowUiEventService } from './workflow-ui-event.service';

describe('WorkflowUiEventService', () => {
  beforeEach(() => {
    TestBed.configureTestingModule({
      providers: [WorkflowUiEventService]
    });
  });

  it('should be created', inject([WorkflowUiEventService], (service: WorkflowUiEventService) => {
    expect(service).toBeTruthy();
  }));
});
