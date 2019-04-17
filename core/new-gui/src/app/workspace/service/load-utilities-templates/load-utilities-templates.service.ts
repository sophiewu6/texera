import { Point } from './../../types/workflow-common.interface';
import { Injectable } from '@angular/core';
import { WorkflowActionService } from './../workflow-graph/model/workflow-action.service';
import { Subject } from 'rxjs/Subject';
import { Observable } from 'rxjs';
import { keyWordSearchPredicate, wordCountPredicate,
  keyWordSearchWordCountLink, viewResultPredicate,
    wordCountViewResultLink } from './../../service/workflow-graph/model/mock-workflow-data';
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

  public createNewOperator(utilityIndex: number, paperSize: { x: number, y: number }) {
    const firstOperatorOffset: Point = {
      x:  paperSize.x * 2,
      y:  paperSize.y * 4
    };
    const secondOperatorOffset: Point = {
      x:  paperSize.x * 3,
      y:  paperSize.y * 4
    };
    const thirdOperatorOffset: Point = {
      x:  paperSize.x * 4,
      y:  paperSize.y * 4
    };
    if (utilityIndex === 0) {
        this.workflowActionService.addOperator(keyWordSearchPredicate, firstOperatorOffset);
        this.workflowActionService.addOperator(wordCountPredicate, secondOperatorOffset);
        this.workflowActionService.addOperator(viewResultPredicate, thirdOperatorOffset);
        this.workflowActionService.addLink(keyWordSearchWordCountLink);
        this.workflowActionService.addLink(wordCountViewResultLink);
    }
  }
}
