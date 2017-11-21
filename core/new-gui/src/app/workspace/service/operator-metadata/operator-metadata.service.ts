import { Injectable } from '@angular/core';
import { Http } from '@angular/http';
import { Observable, Subject } from 'rxjs/Rx';
import '../../../common/rxjs-operators.ts';

import * as _ from 'lodash';

import { OperatorSchema } from '../../model/operator-schema';
import { OperatorPredicate } from '../../model/operator-predicate';
import { OPERATOR_METADATA } from './mock-operator-metadata';

@Injectable()
export class OperatorMetadataService {

  private operatorMetadataList: OperatorSchema[] = [];

  constructor(private http: Http) { }

  private onMetadataChangedSubject = new Subject<OperatorSchema[]>();
  metadataChanged$ = this.onMetadataChangedSubject.asObservable();

  private fetchAllOperatorMetadata(): void {
    Observable.of(OPERATOR_METADATA).subscribe(x => {
      this.operatorMetadataList = x;
      this.onMetadataChangedSubject.next(x);
    });
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
