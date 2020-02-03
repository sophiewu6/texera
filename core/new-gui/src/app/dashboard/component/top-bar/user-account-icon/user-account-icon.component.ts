import { Component, OnInit } from '@angular/core';
import { UserAccountService } from 'src/app/dashboard/service/user-account/user-account.service';
import { NgbModal } from '@ng-bootstrap/ng-bootstrap';

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

  private userName: string = this.getDefaultUserName();

  constructor(
    private modalService: NgbModal,
    private userAccountService: UserAccountService
  ) {}

  ngOnInit() {
    this.subscribeFromUser();
  }


  public registerButton() {
  }

  public loginInButton() {
    const modalRef = this.modalService.open(NgbdModalResourceAddComponent);
  }

  private subscribeFromUser(): void {
    this.userAccountService.getUserChangeEvent()
    .subscribe(
      () => {
        if (this.userAccountService.isLoginIn()) {
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
