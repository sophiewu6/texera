import { TestBed, inject } from '@angular/core/testing';

import { LoadUtilitiesTemplatesService } from './load-utilities-templates.service';

describe('LoadUtilitiesTemplatesService', () => {
  beforeEach(() => {
    TestBed.configureTestingModule({
      providers: [LoadUtilitiesTemplatesService]
    });
  });

  it('should be created', inject([LoadUtilitiesTemplatesService], (service: LoadUtilitiesTemplatesService) => {
    expect(service).toBeTruthy();
  }));
});
