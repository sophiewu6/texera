import { Component, OnInit } from '@angular/core';
import { UserInfoService } from 'src/app/dashboard/service/user-info/user-info.service';

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

  constructor(private userInfoService: UserInfoService) {
  }

  ngOnInit() {
  }

  public registerButton() {
  }

  public loginButton() {

  }

}
