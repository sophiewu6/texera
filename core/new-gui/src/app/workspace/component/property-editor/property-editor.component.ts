import { Component, OnInit, ChangeDetectorRef } from '@angular/core';

import { Observable, Subject } from 'rxjs/Rx';
import '../../../common/rxjs-operators.ts';

import * as _ from 'lodash';

import { OperatorPredicate } from '../../model/operator-predicate';
import { WorkflowDataService } from '../../service/current-workflow/workflow-data.service';
import { OperatorSchema } from '../../model/operator-schema';
import { OperatorMetadataService } from '../../service/operator-metadata/operator-metadata.service';
import { WorkflowUIService } from '../../service/current-workflow/workflow-ui.service';

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
  jsonSchemaObject: Object = undefined;
  formLayout: object = this.generateFormLayout();

  constructor(private workflowDataService: WorkflowDataService,
    private workflowUIService: WorkflowUIService, private operatorMetadataService: OperatorMetadataService,
    private changeDetectorRef: ChangeDetectorRef) {
    this.workflowUIService.operatorSelected$.subscribe(x => this.changePropertyEditor(x));
  }

  ngOnInit() {
  }

  changePropertyEditor(operatorID: string) {
    // console.log('changePropertyEditor called');
    this.currentPredicate = this.workflowDataService.workflowLogicalPlan.getOperator(operatorID);
    this.currentSchema = this.operatorMetadataService.getOperatorMetadata(this.currentPredicate.operatorType);
    this.jsonSchemaObject = this.currentSchema.generateSchemaObject();
    this.changeDetectorRef.detectChanges();
  }

  // layout for the form
  generateFormLayout(): Object {
    // hide the submit button
    // console.log('generate form layout');
    return [
      '*',
      { type: 'submit', display: false }
    ];
  }

  onFormChanges(event: Object) {
    // console.log('onform changes called');
    // console.log(event);
    this.workflowDataService.changeOperatorProperty(this.currentPredicate.operatorID, event);
  }

}
