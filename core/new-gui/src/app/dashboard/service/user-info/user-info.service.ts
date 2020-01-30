import { Injectable } from '@angular/core';
import { HttpClient, HttpHeaders } from '@angular/common/http';

import { Observable } from 'rxjs/Observable';
import { environment } from '../../../../environments/environment';
import { EventEmitter } from '@angular/core';
import { observable } from 'rxjs';

const registerURL = 'users/register';
const loginInURL = 'users/loginIn';

export interface UserInfo extends Readonly<{
  userName: string;
  ID: number;
}> {}

@Injectable()
export class UserInfoService {
  private userChangeEvent: EventEmitter<UserInfo | null> = new EventEmitter();
  private currentUser: UserInfo | null = null;

  constructor(private http: HttpClient) { }

  public registerUser(userName: string): Observable<UserInfo | null> {
    if (this.isLoginIn()) {
      throw new Error('Already logged in when register.');
    }

    return this.register(userName).map(
      res => {
        if (res != null) {
          this.currentUser = res;
          this.userChangeEvent.emit(this.currentUser);
          return res;
        } else {
          return null;
        }
      }
    );
  }

  public loginInUser(userName: string):  Observable<UserInfo | null> {
    if (this.isLoginIn()) {
      throw new Error('Already logged in when login in.');
    }

    return this.loginIn(userName).map(
      res => {
        if (res != null) {
          this.currentUser = res;
          this.userChangeEvent.emit(this.currentUser);
          return res;
        } else {
          return null;
        }
      }
    );
  }

  public logOut(): void {
    this.currentUser = null;
    this.userChangeEvent.emit(this.currentUser);
  }

  public isLoginIn(): boolean {
    return this.currentUser != null;
  }

  public getCurrentUser(): UserInfo | null {
    return this.currentUser;
  }

  public getUserChangeEvent(): EventEmitter<UserInfo | null> {
    return this.userChangeEvent;
  }


  private register(userName: string): Observable<UserInfo | null> {
    return this.http.put<UserInfo | null>(`${environment.apiUrl}/${registerURL}`,
      {userName}
    );
  }

  private loginIn(userName: string): Observable<UserInfo | null> {
    return this.http.post<UserInfo | null>(`${environment.apiUrl}/${loginInURL}`,
      {userName}
    );
  }

}
