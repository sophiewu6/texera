import { Component, OnInit, ChangeDetectorRef } from '@angular/core';

import { Observable, Subject } from 'rxjs/Rx';
import '../../../common/rxjs-operators.ts';

import * as _ from 'lodash';

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

  operatorID: string = undefined;
  initialData: Object = undefined;
  currentSchema: OperatorSchema = undefined;
  formLayout: object = this.generateFormLayout();

  formChangeTimes = 0;


  constructor(
    private operatorMetadataService: OperatorMetadataService,
    private workflowModelSerivce: WorkflowModelService,
    private workflowUIChangeService: WorkflowUIChangeService,
    private workflowDataChangeService: WorkflowDataChangeService) {

    this.workflowUIChangeService.operatorSelected$.distinctUntilChanged().subscribe(x => this.changePropertyEditor(x));
  }

  ngOnInit() {
  }

  changePropertyEditor(operatorID: string) {
    console.log('changePropertyEditor called');
    this.operatorID = operatorID;
    const operatorPredicate = this.workflowModelSerivce.logicalPlan.getOperator(operatorID);
    this.currentSchema = this.operatorMetadataService.getOperatorMetadata(operatorPredicate.operatorType);
    // make a copy of the property data
    this.initialData = Object.assign({}, operatorPredicate.operatorProperties);
    console.log('current predicate properties: ');
    console.log(this.initialData);
    console.log('current json schema');
    console.log(this.currentSchema.jsonSchema);
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
    // console.log('onform changes called');
    // console.log(event);
    // console.log('called ' + this.formChangeTimes.toString() + ' times');
    this.workflowDataChangeService.changeProperty(this.operatorID, event);
  }

}
