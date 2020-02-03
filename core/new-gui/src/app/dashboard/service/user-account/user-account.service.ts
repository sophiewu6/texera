import { Injectable } from '@angular/core';
import { HttpClient, HttpHeaders } from '@angular/common/http';

import { Observable } from 'rxjs/Observable';
import { environment } from '../../../../environments/environment';
import { EventEmitter } from '@angular/core';
import { observable } from 'rxjs';
import { UserAccount } from '../../type/user-account';
import { UserAccountResponse } from '../../type/user-account';

const registerURL = 'users/accounts/register';
const loginURL = 'users/accounts/login';


@Injectable()
export class UserAccountService {
  private userChangeEvent: EventEmitter<UserAccount> = new EventEmitter();
  private currentUser: UserAccount = this.createEmptyUser();
  private isLoginFlag: boolean = false;

  constructor(private http: HttpClient) { }

  public registerUser(userName: string): Observable<UserAccountResponse> {
    if (this.isLogin()) {
      throw new Error('Already logged in when register.');
    }

    return this.register(userName).map(
      res => {
        if (res.code === 0) {
          this.currentUser = res.userAccount;
          this.userChangeEvent.emit(this.currentUser);
          this.isLoginFlag = true;
          return res;
        } else { // register failed
          return res;
        }
      }
    );
  }

  public loginUser(userName: string):  Observable<UserAccountResponse> {
    if (this.isLogin()) {
      throw new Error('Already logged in when login in.');
    }

    return this.login(userName).map(
      res => {
        if (res.code === 0) {
          this.currentUser = res.userAccount;
          this.userChangeEvent.emit(this.currentUser);
          this.isLoginFlag = true;
          return res;
        } else { // login in failed
          return res;
        }
      }
    );
  }

  public logOut(): void {
    this.isLoginFlag = false;
    this.currentUser = this.createEmptyUser();
    this.userChangeEvent.emit(this.currentUser);
  }

  public isLogin(): boolean {
    return this.isLoginFlag;
  }

  public getCurrentUser(): UserAccount {
    return this.currentUser;
  }

  public getUserChangeEvent(): EventEmitter<UserAccount> {
    return this.userChangeEvent;
  }


  private register(userName: string): Observable<UserAccountResponse> {
    return this.http.put<UserAccountResponse>(`${environment.apiUrl}/${registerURL}`,
      {userName}
    );
  }

  private login(userName: string): Observable<UserAccountResponse> {
    return this.http.get<UserAccountResponse>(`${environment.apiUrl}/${loginURL}`,
      {params : {name : userName}}
    );
  }

  private createEmptyUser(): UserAccount {
    const emptyUser: UserAccount = {
      userName : '',
      userID : -1
    };
    return emptyUser;
  }

}
