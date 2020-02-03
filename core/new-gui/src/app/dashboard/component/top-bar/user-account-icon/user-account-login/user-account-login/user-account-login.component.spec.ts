import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { UserAccountLoginComponent } from './user-account-login.component';

describe('UserAccountLoginComponent', () => {
  let component: UserAccountLoginComponent;
  let fixture: ComponentFixture<UserAccountLoginComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ UserAccountLoginComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(UserAccountLoginComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
