import { TestBed, inject } from '@angular/core/testing';

import { WorkflowUIService } from './workflow-ui.service';

describe('WorkflowUIService', () => {
  beforeEach(() => {
    TestBed.configureTestingModule({
      providers: [WorkflowUIService]
    });
  });

  it('should be created', inject([WorkflowUIService], (service: WorkflowUIService) => {
    expect(service).toBeTruthy();
  }));
});
