import { Injectable } from '@angular/core';
import { Http } from '@angular/http';
import { Observable } from 'rxjs/Rx';
import '../../../common/rxjs-operators.ts';

import OperatorSchema from '../../model/operator-schema';
import { OPERATOR_METADATA } from './mock-operator-metadata';

@Injectable()
export class OperatorMetadataService {

  private operatorMetadataMap: Map<string, OperatorSchema>;

  constructor(private http: Http) { }

  private fetchAllOperatorMetadata(): Observable<OperatorSchema[]> {
    return Observable.of(OPERATOR_METADATA);
  }

  getOperatorMetadataMap(): Observable<Map<string, OperatorSchema>> {
    if (! this.operatorMetadataMap) {
      const resultObservable = this.fetchAllOperatorMetadata()
        .map(schemaList => new Map<string, OperatorSchema>(
          schemaList.map<[string, OperatorSchema]>((i) => [i.operatorType, i])));

      resultObservable.subscribe(result => {
        this.operatorMetadataMap = result;
        console.log('subscribe called, this.operatorMetadataMap value is set');
        console.log(result);
      });
      return resultObservable;
    } else {
      return Observable.of(this.operatorMetadataMap);
    }
  }

  getOperatorMetadata(operatorType: string): Observable<OperatorSchema> {
    return null;
  }


}
