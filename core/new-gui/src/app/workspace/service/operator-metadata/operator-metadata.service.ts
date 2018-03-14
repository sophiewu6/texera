import { Injectable } from '@angular/core';
import { Http } from '@angular/http';
import { Observable, Subject } from 'rxjs/Rx';
import '../../../common/rxjs-operators.ts';

import { OPERATOR_METADATA } from './mock-operator-metadata';
import { AppSettings } from '../../../common/app-setting';

export const OPERATOR_METADATA_ENDPOINT = 'resources/operator-metadata';


/**
 * OperatorMetadataService talks to the backend to fetch the operator metadata,
 *  which contains a list of operator schemas.
 * Each operator schema contains all the information related to an operator,
 *  for example, operatorType, userFriendlyName, and the jsonSchema of its properties.
 *
 * fetchAllOperatorMetadata is called in ngOnInit function of the root component,
 *  to initiate the HTTP request to get the data.
 *
 * If a Component or a Service wants to get the operatorMetadata, it should
 *  subscribe to the metadataChanged Observable to get the metadata,
 *  once the metadata is ready, it will be broadcasted through the observable.
 *
 * @author Zuozhi Wang
 *
 */
@Injectable()
export class OperatorMetadataService {

  private operatorMetadata: OperatorSchema[] = [];

  constructor(private http: Http) { }

  private onMetadataChangedSubject = new Subject<OperatorSchema[]>();
  public metadataChanged$ = this.onMetadataChangedSubject.asObservable();

  public fetchAllOperatorMetadata(): void {
    this.http.get(`${AppSettings.API_ENDPOINT}/${OPERATOR_METADATA_ENDPOINT}`).subscribe(
      value => {
        this.operatorMetadata = value.json();
        this.onMetadataChangedSubject.next(this.operatorMetadata);
      }
    );
  }

}
