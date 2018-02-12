import { Injectable } from '@angular/core';
import { Http } from '@angular/http';
import { Observable, Subject } from 'rxjs/Rx';
import '../../../common/rxjs-operators.ts';

import * as _ from 'lodash';

import { OperatorSchema } from '../../model/operator-schema';
import { OperatorPredicate } from '../../model/operator-predicate';
import { OPERATOR_METADATA } from './mock-operator-metadata';
import { AppSettings } from '../../../common/app-setting';
import { PropertySchema } from '../../model/property-schema';

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
        this.operatorMetadataList = this.transformOperatorSchema(value.json());
        this.onMetadataChangedSubject.next(this.operatorMetadataList);
      }
    );
  }

  private transformOperatorSchema(value: Object[]): OperatorSchema[] {
    console.log(value);

    const operatorSchema: OperatorSchema[] = [];
    value.forEach(
      op => operatorSchema.push(
        new OperatorSchema(
          op['operatorType'],
          op['userFriendlyName'],
          this.transformPropertySchema(op['properties']),
          op['inputNumber'],
          op['outputNumber'],
          op['operatorDescription'],
          op['required'],
          op['advancedOptions'],
          op['operatorGroupName']
        ))
    );
    return operatorSchema;
  }

  private transformPropertySchema(properties: Object): PropertySchema[] {
    const propertySchema: PropertySchema[] = [];
    Object.keys(properties).forEach(
      property => propertySchema.push(
        new PropertySchema(
          property,
          properties[property]['type']
        ))
    );
    return propertySchema;
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
