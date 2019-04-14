import { Point } from './../../types/workflow-common.interface';
import { Injectable } from '@angular/core';
import { WorkflowActionService } from './../workflow-graph/model/workflow-action.service';
import { Subject } from 'rxjs/Subject';
import { Observable } from 'rxjs';

@Injectable({
  providedIn: 'root'
})
export class LoadUtilitiesTemplatesService {

  private utilitiesTemplateSubject: Subject<number> = new Subject<number>();
  constructor(private workflowActionService: WorkflowActionService) { }

  public getUtilitiesTemplatesSubject(): Observable<number> {
    return this.utilitiesTemplateSubject.asObservable();
  }

  public setDeleteValue(value: number): void {
    this.utilitiesTemplateSubject.next(value);
  }


}
