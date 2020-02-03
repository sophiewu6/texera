import { Component, OnInit } from '@angular/core';
import { NgbActiveModal } from '@ng-bootstrap/ng-bootstrap';
import { UserAccountService } from '../../../../../service/user-account/user-account.service';

@Component({
  selector: 'texera-user-account-login',
  templateUrl: './user-account-login.component.html',
  styleUrls: ['./user-account-login.component.scss']
})
export class NgbdModalUserAccountLoginComponent implements OnInit {
  public loginUserName: string = '';
  public registerUserName: string = '';


  constructor(
    public activeModal: NgbActiveModal,
    private userAccountService: UserAccountService) { }

  ngOnInit() {
  }

  public login(): void {
    if (this.loginUserName.length === 0) {
      return;
    }

    this.userAccountService.loginUser(this.loginUserName)
      .subscribe(
        res => {
          if (res.code === 0) { // successfully login in
            this.activeModal.close();
          } else { // login error
            // TODO
            console.log(res.message);
          }
        }
      );
  }

  public register(): void {
    if (this.registerUserName.length === 0) {
      return;
    }

    this.userAccountService.registerUser(this.loginUserName)
      .subscribe(
        res => {
          if (res.code === 0) { // successfully register
            this.activeModal.close();
          } else { // register error
            // TODO
            console.log(res.message);
          }
        }
      );
  }



}
