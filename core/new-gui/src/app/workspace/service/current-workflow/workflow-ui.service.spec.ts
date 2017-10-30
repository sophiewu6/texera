import { TestBed, inject } from '@angular/core/testing';

import { WorkflowUiService } from './workflow-ui.service';

describe('WorkflowUiService', () => {
  beforeEach(() => {
    TestBed.configureTestingModule({
      providers: [WorkflowUiService]
    });
  });

  it('should be created', inject([WorkflowUiService], (service: WorkflowUiService) => {
    expect(service).toBeTruthy();
  }));
});
