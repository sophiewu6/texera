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

  private fetchAllOperatorMetadata(): void {
    // Observable.of(OPERATOR_METADATA).subscribe(x => {
    //   this.operatorMetadataList = x;
    //   this.onMetadataChangedSubject.next(x);
    // });

    this.http.get(`${AppSettings.API_ENDPOINT}/${OPERATOR_METADATA_ENDPOINT}`).subscribe(
      value => {
        this.operatorMetadataList = value.json();
        this.onMetadataChangedSubject.next(this.operatorMetadataList);
      }
    );
  }

  getOperatorMetadataList(): OperatorSchema[] {
    if (this.operatorMetadataList.length === 0) {
      this.fetchAllOperatorMetadata();
    }
    return this.operatorMetadataList;
  }

  getOperatorMetadata(operatorType: string): OperatorSchema {
    return this.getOperatorMetadataList().find(x => x.operatorType === operatorType);
  }

}
