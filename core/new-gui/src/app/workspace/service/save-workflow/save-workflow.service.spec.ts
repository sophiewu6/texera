import { TestBed, inject } from '@angular/core/testing';

import { SaveWorkflowService } from './save-workflow.service';

describe('SaveWorkflowService', () => {
  beforeEach(() => {
    TestBed.configureTestingModule({
      providers: [SaveWorkflowService]
    });
  });

  it('should be created', inject([SaveWorkflowService], (service: SaveWorkflowService) => {
    expect(service).toBeTruthy();
  }));
});
