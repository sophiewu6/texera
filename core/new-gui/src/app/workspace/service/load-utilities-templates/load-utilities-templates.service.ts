import { Point } from './../../types/workflow-common.interface';
import { Injectable } from '@angular/core';
import { WorkflowActionService } from './../workflow-graph/model/workflow-action.service';
import {  scanKeyWordSearchLink,  scanPredicate,
  keyWordSearchPredicate, keyWordSearchViewResultLink,
    viewResultPredicate } from './../../service/workflow-graph/model/mock-workflow-data';
@Injectable({
  providedIn: 'root'
})
export class LoadUtilitiesTemplatesService {

  constructor(private workflowActionService: WorkflowActionService) { }

  public createKeyWordsTemplate(paperSize: { x: number, y: number }) {
    console.log(paperSize);
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
    this.workflowActionService.addOperator(scanPredicate, firstOperatorOffset);
    this.workflowActionService.addOperator(keyWordSearchPredicate, secondOperatorOffset);
    this.workflowActionService.addOperator(viewResultPredicate, thirdOperatorOffset);
    this.workflowActionService.addLink(scanKeyWordSearchLink);
    this.workflowActionService.addLink(keyWordSearchViewResultLink);
  }

}
