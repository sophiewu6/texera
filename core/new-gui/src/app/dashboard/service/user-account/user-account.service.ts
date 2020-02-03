import { Injectable } from '@angular/core';
import { HttpClient, HttpHeaders } from '@angular/common/http';

import { Observable } from 'rxjs/Observable';
import { environment } from '../../../../environments/environment';
import { EventEmitter } from '@angular/core';
import { observable } from 'rxjs';
import { UserAccount } from '../../type/user-account';
import { UserAccountResponse } from '../../type/user-account-response';

const registerURL = 'users/dictionaries/register';
const loginInURL = 'users/dictionaries/loginIn';


@Injectable()
export class UserInfoService {
  private userChangeEvent: EventEmitter<UserAccount> = new EventEmitter();
  private currentUser: UserAccount = this.createEmptyUser();
  private isLoginInFlag: boolean = false;

  constructor(private http: HttpClient) { }

  public registerUser(userName: string): Observable<UserAccountResponse> {
    if (this.isLoginIn()) {
      throw new Error('Already logged in when register.');
    }

    return this.register(userName).map(
      res => {
        if (res.code === 0) {
          this.currentUser = res.userAccount;
          this.userChangeEvent.emit(this.currentUser);
          this.isLoginInFlag = true;
          return res;
        } else {
          return res;
        }
      }
    );
  }

  public loginInUser(userName: string):  Observable<UserAccountResponse> {
    if (this.isLoginIn()) {
      throw new Error('Already logged in when login in.');
    }

    return this.loginIn(userName).map(
      res => {
        if (res.code === 0) {
          this.currentUser = res.userAccount;
          this.userChangeEvent.emit(this.currentUser);
          this.isLoginInFlag = true;
          return res;
        } else {
          return res;
        }
      }
    );
  }

  public logOut(): void {
    this.isLoginInFlag = false;
    this.currentUser = this.createEmptyUser();
    this.userChangeEvent.emit(this.currentUser);
  }

  public isLoginIn(): boolean {
    return this.isLoginInFlag;
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

  private loginIn(userName: string): Observable<UserAccountResponse> {
    return this.http.get<UserAccountResponse>(`${environment.apiUrl}/${loginInURL}`,
      {params : {name : userName}}
    );
  }

  private createEmptyUser(): UserAccount {
    const emptyUser: UserAccount = {
      userName : '',
      ID : -1
    };
    return emptyUser;
  }

}
