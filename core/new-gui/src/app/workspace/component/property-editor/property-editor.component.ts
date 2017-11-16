import { Component, OnInit } from '@angular/core';

import { Observable, Subject } from 'rxjs/Rx';
import '../../../common/rxjs-operators.ts';

import * as _ from 'lodash';

import { OperatorPredicate } from '../../model/operator-predicate';
import { WorkflowDataService } from '../../service/current-workflow/workflow-data.service';
import { OperatorSchema } from '../../model/operator-schema';
import { OperatorMetadataService } from '../../service/operator-metadata/operator-metadata.service';

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

  private formChangedSubject = new Subject<Object>();
  formChanged$ = this.formChangedSubject.asObservable();

  constructor(private workflowDataService: WorkflowDataService, private operatorMetadataService: OperatorMetadataService) {
    this.workflowDataService.operatorSelected$.subscribe(x => this.changePropertyEditor(x[0]));
    this.formChanged$.debounceTime(100).distinctUntilChanged((a, b) => _.isEqual(a, b)).subscribe(x => this.formChanged(x));
  }

  ngOnInit() {
  }

  async changePropertyEditor(operatorPredicate: OperatorPredicate) {
    this.currentPredicate = operatorPredicate;
    this.currentSchema = await this.operatorMetadataService.getOperatorMetadata(operatorPredicate.operatorType);
    this.jsonSchemaObject = this.currentSchema.generateSchemaObject();
  }

  // layout for the form
  generateFormLayout(): Object {
    // hide the submit button
    console.log('generate form layout');
    return [
      '*',
      { type: 'submit', display: false }
    ];
  }

  onFormChanges(event: Object) {
    this.formChangedSubject.next(event);
  }

  formChanged(change: Object) {
    console.log(change);
  }

}
