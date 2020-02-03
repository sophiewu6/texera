import { Component, OnInit } from '@angular/core';
import { UserAccountService } from 'src/app/dashboard/service/user-account/user-account.service';

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

  userName = 'User';

  constructor(private userAccountService: UserAccountService) {
  }

  ngOnInit() {
  }

  public registerButton() {
  }

  public loginButton() {

  }

}
