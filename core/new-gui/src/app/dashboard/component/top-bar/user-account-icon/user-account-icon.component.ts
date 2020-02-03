import { Component, OnInit } from '@angular/core';
import { UserAccountService } from 'src/app/dashboard/service/user-account/user-account.service';
import { NgbModal, NgbModalRef } from '@ng-bootstrap/ng-bootstrap';
import { UserAccountLoginComponent } from './user-account-login/user-account-login/user-account-login.component';

/**
 * UserAccountIconComponent is triggered when user wants to log into the system
 *
 * //this component is currently unavailable to use//
 *
 * @author Zhaomin Li
 */
@Component({
  selector: 'texera-user-account-icon',
  templateUrl: './user-account-icon.component.html',
  styleUrls: ['./user-account-icon.component.scss']
})
export class UserAccountIconComponent implements OnInit {
  public userName: string = this.getDefaultUserName();

  constructor(
    private modalService: NgbModal,
    private userAccountService: UserAccountService
  ) {}

  ngOnInit() {
    this.subscribeFromUser();
  }

  public logOutButton(): void {
    this.userAccountService.logOut();
  }

  public logInButton(): void {
    this.openLoginInComponent(0);
  }

  public registerButton(): void {
    this.openLoginInComponent(1);
  }

  public isLogIn() {
    return this.userAccountService.isLogIn();
  }

  private openLoginInComponent(mode: number): void {
    const modalRef: NgbModalRef = this.modalService.open(UserAccountLoginComponent);

    this.userAccountService.getUserChangeEvent()
    .subscribe(
      () => {
        if (this.userAccountService.isLogIn()) {
          try {
            modalRef.close();
          } catch (e) {}
        }
      }
    );
  }

  private subscribeFromUser(): void {
    this.userAccountService.getUserChangeEvent()
    .subscribe(
      () => {
        if (this.userAccountService.isLogIn()) {
          this.userName = this.userAccountService.getCurrentUser().userName;
        } else {
          this.userName = this.getDefaultUserName();
        }
      }
    );

  }

  private getDefaultUserName(): string {
    return 'User';
  }

}
