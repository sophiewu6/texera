import { Injectable } from '@angular/core';
import { Http } from '@angular/http';
import { Observable } from 'rxjs/Rx';
import '../../../common/rxjs-operators.ts';

import * as _ from 'lodash';

import { OperatorSchema } from '../../model/operator-schema';
import { OperatorPredicate } from '../../model/operator-predicate';
import { OPERATOR_METADATA } from './mock-operator-metadata';

@Injectable()
export class OperatorMetadataService {

  private operatorMetadataList: OperatorSchema[];

  constructor(private http: Http) { }

  private fetchAllOperatorMetadata(): Promise<OperatorSchema[]> {
    return Observable.of(OPERATOR_METADATA).toPromise();
  }

  async getOperatorMetadataList(): Promise<OperatorSchema[]> {
    if (! this.operatorMetadataList) {
      this.operatorMetadataList = await this.fetchAllOperatorMetadata();
      return this.operatorMetadataList;
    }
    return Observable.of(this.operatorMetadataList).toPromise();
  }

  async getOperatorMetadata(operatorType: string): Promise<OperatorSchema> {
    const operatorMetadataList = await this.getOperatorMetadataList();
    return operatorMetadataList.find(x => x.operatorType === operatorType);
  }

}
