import { TestBed, inject } from '@angular/core/testing';

import { LoadUtilitiesTemplatesService } from './load-utilities-templates.service';
import { WorkflowActionService } from './../workflow-graph/model/workflow-action.service';
import { OperatorMetadataService } from '../operator-metadata/operator-metadata.service';
import { StubOperatorMetadataService } from '../operator-metadata/stub-operator-metadata.service';
import { JointUIService } from '../joint-ui/joint-ui.service';

describe('LoadUtilitiesTemplatesService', () => {
  beforeEach(() => {
    TestBed.configureTestingModule({
      providers: [
        LoadUtilitiesTemplatesService,
        WorkflowActionService,
        JointUIService,
        { provide: OperatorMetadataService, useClass: StubOperatorMetadataService },
      ]
    });
  });

  it('should be created', inject([LoadUtilitiesTemplatesService], (service: LoadUtilitiesTemplatesService) => {
    expect(service).toBeTruthy();
  }));
});
