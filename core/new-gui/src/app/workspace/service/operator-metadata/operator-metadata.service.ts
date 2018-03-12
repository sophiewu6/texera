import { Injectable } from '@angular/core';
import { Http } from '@angular/http';
import { Observable, Subject } from 'rxjs/Rx';
import '../../../common/rxjs-operators.ts';

import * as _ from 'lodash';

import { OPERATOR_METADATA } from './mock-operator-metadata';
import { AppSettings } from '../../../common/app-setting';

export const OPERATOR_METADATA_ENDPOINT = 'resources/operator-metadata';

@Injectable()
export class OperatorMetadataService {

  private operatorMetadataList: OperatorSchema[] = [];

  constructor(private http: Http) { }

  private onMetadataChangedSubject = new Subject<OperatorSchema[]>();
  metadataChanged$ = this.onMetadataChangedSubject.asObservable();

  public fetchAllOperatorMetadata(): void {
    this.http.get(`${AppSettings.API_ENDPOINT}/${OPERATOR_METADATA_ENDPOINT}`).subscribe(
      value => {
        this.operatorMetadataList = value.json();
        this.onMetadataChangedSubject.next(this.operatorMetadataList);
      }
    );
  }

}
