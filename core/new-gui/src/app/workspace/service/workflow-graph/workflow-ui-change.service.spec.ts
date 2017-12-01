import { TestBed, inject } from '@angular/core/testing';

import { WorkflowUIChangeService } from './workflow-ui-change.service';

describe('WorkflowUIChangeService', () => {
  beforeEach(() => {
    TestBed.configureTestingModule({
      providers: [WorkflowUIChangeService]
    });
  });

  it('should be created', inject([WorkflowUIChangeService], (service: WorkflowUIChangeService) => {
    expect(service).toBeTruthy();
  }));
});
