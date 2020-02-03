import { Injectable } from '@angular/core';
import { HttpClient, HttpHeaders } from '@angular/common/http';

import { Observable } from 'rxjs/Observable';
import { environment } from '../../../../environments/environment';
import { EventEmitter } from '@angular/core';
import { observable } from 'rxjs';
import { UserAccount } from '../../type/user-account';
import { UserAccountResponse } from '../../type/user-account';

const registerURL = 'users/accounts/register';
const loginInURL = 'users/accounts/logIn';


@Injectable()
export class UserAccountService {
  private userChangeEvent: EventEmitter<UserAccount> = new EventEmitter();
  private currentUser: UserAccount = this.createEmptyUser();
  private isLogInFlag: boolean = false;

  constructor(private http: HttpClient) { }

  public registerUser(userName: string): Observable<UserAccountResponse> {
    if (this.isLogIn()) {
      throw new Error('Already logged in when register.');
    }

    return this.register(userName).map(
      res => {
        if (res.code === 0) {
          this.currentUser = res.userAccount;
          this.userChangeEvent.emit(this.currentUser);
          this.isLogInFlag = true;
          return res;
        } else { // register failed
          return res;
        }
      }
    );
  }

  public logInUser(userName: string):  Observable<UserAccountResponse> {
    if (this.isLogIn()) {
      throw new Error('Already logged in when login in.');
    }

    return this.logIn(userName).map(
      res => {
        if (res.code === 0) {
          this.currentUser = res.userAccount;
          this.userChangeEvent.emit(this.currentUser);
          this.isLogInFlag = true;
          return res;
        } else { // login in failed
          return res;
        }
      }
    );
  }

  public logOut(): void {
    this.isLogInFlag = false;
    this.currentUser = this.createEmptyUser();
    this.userChangeEvent.emit(this.currentUser);
  }

  public isLogIn(): boolean {
    return this.isLogInFlag;
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

  private logIn(userName: string): Observable<UserAccountResponse> {
    return this.http.get<UserAccountResponse>(`${environment.apiUrl}/${loginInURL}`,
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
