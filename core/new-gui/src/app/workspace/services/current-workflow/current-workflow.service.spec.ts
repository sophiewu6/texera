import { TestBed, inject } from '@angular/core/testing';

import { CurrentWorkflowService } from './current-workflow.service';

describe('CurrentWorkflowService', () => {
  beforeEach(() => {
    TestBed.configureTestingModule({
      providers: [CurrentWorkflowService]
    });
  });

  it('should be created', inject([CurrentWorkflowService], (service: CurrentWorkflowService) => {
    expect(service).toBeTruthy();
  }));
});
