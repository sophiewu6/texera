import { Component, OnInit, ChangeDetectorRef } from '@angular/core';

import { Observable, Subject } from 'rxjs/Rx';
import '../../../common/rxjs-operators.ts';

import * as _ from 'lodash';

import { OperatorPredicate } from '../../model/operator-predicate';
import { OperatorSchema } from '../../model/operator-schema';
import { OperatorMetadataService } from '../../service/operator-metadata/operator-metadata.service';
import { WorkflowModelService } from '../../service/workflow-graph/workflow-model.service';
import { WorkflowUIChangeService } from '../../service/workflow-graph/workflow-ui-change.service';
import { WorkflowDataChangeService } from '../../service/workflow-graph/workflow-data-change.service';


@Component({
  selector: 'texera-property-editor',
  templateUrl: './property-editor.component.html',
  styleUrls: ['./property-editor.component.scss']
})
export class PropertyEditorComponent implements OnInit {

  /*
    Disable two-way data binding of currentPredicate
    to prevent "onChanges" event fired continously.
    currentPredicate won't change as the form value changes
  */
  currentPredicate: OperatorPredicate = undefined;
  currentSchema: OperatorSchema = undefined;
  jsonSchemaObject: any = undefined;
  formLayout: object = this.generateFormLayout();

  formChangeTimes = 0;


  exampleSchema = {
    'type': 'object', 'id': 'urn:jsonschema:edu:uci:ics:texera:dataflow:keywordmatcher:KeywordPredicate',
    'properties': {
      'query': { 'type': 'string' },
      'attributes': { 'type': 'array', 'items': { 'type': 'string' } },
      'luceneAnalyzer': { 'type': 'string' },
      'matchingType': { 'type': 'string', 'enum': ['scan', 'conjunction', 'phrase', 'regex'] },
      'spanListName': { 'type': 'string' }
    },
    'operatorType': 'KeywordMatcher', 'inputNumber': 1, 'outputNumber': 1,
    'userFriendlyName': 'Keyword Search',
    'operatorDescription': 'Search the documents using a keyword',
    'operatorGroupName': 'Search', 'required': ['query', 'attributes', 'matchingType'], 'advancedOptions': []
  };


  constructor(
    private operatorMetadataService: OperatorMetadataService,
    private workflowModelSerivce: WorkflowModelService,
    private workflowUIChangeService: WorkflowUIChangeService,
    private workflowDataChangeService: WorkflowDataChangeService) {

    this.workflowUIChangeService.operatorSelected$.distinctUntilChanged().subscribe(x => this.changePropertyEditor(x));
  }

  ngOnInit() {
    this.currentPredicate = Object();
    this.jsonSchemaObject = {
      'type': 'object', 'id': 'urn:jsonschema:edu:uci:ics:texera:dataflow:keywordmatcher:KeywordPredicate',
      'properties': {
        'query': { 'type': 'string' },
        'attributes': { 'type': 'array', 'items': { 'type': 'string' } },
        'luceneAnalyzer': { 'type': 'string' },
        'matchingType': { 'type': 'string', 'enum': ['scan', 'conjunction', 'phrase', 'regex'] },
        'spanListName': { 'type': 'string' }
      },
      'operatorType': 'KeywordMatcher', 'inputNumber': 1, 'outputNumber': 1,
      'userFriendlyName': 'Keyword Search',
      'operatorDescription': 'Search the documents using a keyword',
      'operatorGroupName': 'Search', 'required': ['query', 'attributes', 'matchingType'], 'advancedOptions': []
    };
  }

  changePropertyEditor(operatorID: string) {
    console.log('changePropertyEditor called');
    // this.currentPredicate = this.workflowModelSerivce.logicalPlan.getOperator(operatorID);
    // this.currentSchema = this.operatorMetadataService.getOperatorMetadata(this.currentPredicate.operatorType);
    // this.jsonSchemaObject = this.currentSchema.generateSchemaObject();
  }

  // layout for the form
  generateFormLayout(): Object {
    // hide submit button
    return [
      '*',
      { type: 'submit', display: false }
    ];
  }

  onFormChanges(event: Object) {
    this.formChangeTimes++;
    console.log('onform changes called');
    console.log(event);
    console.log('called ' + this.formChangeTimes.toString() + ' times');
    this.workflowDataChangeService.changeProperty(this.currentPredicate.operatorID, event);
  }

}
