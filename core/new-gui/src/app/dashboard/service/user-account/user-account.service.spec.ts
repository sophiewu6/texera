import { TestBed, inject } from '@angular/core/testing';

import { UserAccountService } from './user-account.service';

describe('UserInfoService', () => {
  beforeEach(() => {
    TestBed.configureTestingModule({
      providers: [UserAccountService]
    });
  });

  it('should be created', inject([UserAccountService], (service: UserAccountService) => {
    expect(service).toBeTruthy();
  }));
});
