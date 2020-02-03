import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { NgbdModalUserAccountLoginComponent } from './user-account-login.component';

describe('UserAccountLoginComponent', () => {
  let component: NgbdModalUserAccountLoginComponent;
  let fixture: ComponentFixture<NgbdModalUserAccountLoginComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ NgbdModalUserAccountLoginComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(NgbdModalUserAccountLoginComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
