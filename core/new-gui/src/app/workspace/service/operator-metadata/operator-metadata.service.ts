import { Injectable } from '@angular/core';
import { Http } from '@angular/http';
import { Observable } from 'rxjs/Rx';
import '../../../common/rxjs-operators.ts';

import { OperatorSchema } from '../../model/operator-schema';
import { OPERATOR_METADATA } from './mock-operator-metadata';

@Injectable()
export class OperatorMetadataService {

  private operatorMetadataList: OperatorSchema[];

  constructor(private http: Http) { }

  private fetchAllOperatorMetadata(): Observable<OperatorSchema[]> {
    return Observable.of(OPERATOR_METADATA);
  }

  getOperatorMetadataList(): Observable<OperatorSchema[]> {
    if (! this.operatorMetadataList) {
      const resultObservable = this.fetchAllOperatorMetadata();
      resultObservable.subscribe(result => this.operatorMetadataList = result);

      return resultObservable;
    }
    return Observable.of(this.operatorMetadataList);
  }

}
