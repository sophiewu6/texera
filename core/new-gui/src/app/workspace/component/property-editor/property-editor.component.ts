import { OperatorSchema } from './../../types/operator-schema.interface';
import { OperatorPredicate } from '../../types/workflow-common.interface';
import { WorkflowActionService } from './../../service/workflow-graph/model/workflow-action.service';
import { OperatorMetadataService } from './../../service/operator-metadata/operator-metadata.service';
import { Component, Input } from '@angular/core';

import { Subject } from 'rxjs/Subject';
import { Observable } from 'rxjs/Observable';
import '../../../common/rxjs-operators';

// all lodash import should follow this parttern
// import `functionName` from `lodash-es/functionName`
// to import only the function that we use
import cloneDeep from 'lodash-es/cloneDeep';
import isEqual from 'lodash-es/isEqual';


import {NgbModal, NgbActiveModal} from '@ng-bootstrap/ng-bootstrap';

/**
 * PropertyEditorComponent is the panel that allows user to edit operator properties.
 *
 * Property Editor uses JSON Schema to automatically generate the form from the JSON Schema of an operator.
 * For example, the JSON Schema of Sentiment Analysis could be:
 *  'properties': {
 *    'attribute': { 'type': 'string' },
 *    'resultAttribute': { 'type': 'string' }
 *  }
 * The automatically generated form will show two input boxes, one titled 'attribute' and one titled 'resultAttribute'.
 * More examples of the operator JSON schema can be found in `mock-operator-metadata.data.ts`
 * More about JSON Schema: Understading JSON Schema - https://spacetelescope.github.io/understanding-json-schema/
 *
 * OperatorMetadataService will fetch metadata about the operators, which includes the JSON Schema, from the backend.
 *
 * We use library `angular2-json-schema-form` to generate form from json schema
 * https://github.com/dschnelldavis/angular2-json-schema-form
 *
 * For more details of comparing different libraries, and the problems of the current library,
 *  see `json-schema-library.md`
 *
 * @author Zuozhi Wang
 */
@Component({
  selector: 'texera-property-editor',
  templateUrl: './property-editor.component.html',
  styleUrls: ['./property-editor.component.scss'],
})
export class PropertyEditorComponent {

  // debounce time for form input in miliseconds
  //  please set this to multiples of 10 to make writing tests easy
  public static formInputDebounceTime: number = 150;

  // the operatorID corresponds to the property editor's current operator
  public currentOperatorID: string | undefined;
  // a *copy* of the operator property as the initial input to the json schema form
  // see details of why making a copy below at where the copy is made
  public currentOperatorInitialData: object | undefined;
  // the operator schema of the current operator
  public currentOperatorSchema: OperatorSchema | undefined;
  // used in HTML template to control if the form is displayed
  public displayForm: boolean = false;

  // the form layout passed to angular json schema library to hide *submit* button
  public formLayout: object = PropertyEditorComponent.generateFormLayout();

  // the source event stream of form change triggered by library at each user input
  public sourceFormChangeEventStream = new Subject<object>();

  // the output form change event stream after debouce time and filtering out values
  public outputFormChangeEventStream = this.createOutputFormChangeEventStream(this.sourceFormChangeEventStream);

  // the current operator schema list, used to find the operator schema of current operator
  public operatorSchemaList: ReadonlyArray<OperatorSchema> = [];

  public property_description: Map<String, String> | undefined;


  constructor(
    private operatorMetadataService: OperatorMetadataService,
    private workflowActionService: WorkflowActionService,
    private modalService: NgbModal
  ) {
    // handle getting operator metadata
    this.operatorMetadataService.getOperatorMetadata().subscribe(
      value => { this.operatorSchemaList = value.operators; }
    );

    // handle the form change event to actually set the operator property
    this.outputFormChangeEventStream.subscribe(formData => {
      // set the operator property to be the new form data
      if (this.currentOperatorID) {
        this.workflowActionService.setOperatorProperty(this.currentOperatorID, formData);
      }
    });

    // handle highlight / unhighlight event to show / hide the property editor form
    this.handleHighlightEvents();


  

  }

  /**
   * Callback function provided to the Angular Json Schema Form library,
   *  whenever the form data is changed, this function is called.
   * It only serves as a bridge from a callback function to RxJS Observable
   * @param formData
   */
  public onFormChanges(formData: object): void {
    this.sourceFormChangeEventStream.next(formData);
  }

  /**
   * Hides the form and clears all the data of the current the property editor
   */
  public clearPropertyEditor(): void {
    // set displayForm to false in the very beginning
    // hide the view first and then make everything null
    this.displayForm = false;

    this.currentOperatorID = undefined;
    this.currentOperatorInitialData = undefined;
    this.currentOperatorSchema = undefined;

  }

  /**
   * Changes the property editor to use the new operator data.
   * Sets all the data needed by the json schema form and displays the form.
   * @param operator
   */
  public changePropertyEditor(operator: OperatorPredicate | undefined): void {
    if (!operator) {
      throw new Error(`change property editor: operator is undefined`);
    }

    // set displayForm to false first to hide the view while constructing data
    this.displayForm = false;

    // set the operator data needed
    this.currentOperatorID = operator.operatorID;
    this.currentOperatorSchema = this.operatorSchemaList.find(schema => schema.operatorType === operator.operatorType);

    if (!this.currentOperatorSchema) {
      throw new Error(`operator schema for operator type ${operator.operatorType} doesn't exist`);
    }

    this.property_description = new Map(Object.entries(this.currentOperatorSchema.additionalMetadata.property_description));

    /**
     * Make a deep copy of the initial property data object.
     * It's important to make a deep copy. If it's a reference to the operator's property object,
     *  form change event -> property object change -> input to form change -> form change event
     *  although the it falls into an infinite loop of tirggering events.
     * Making a copy prevents that property object change triggers the input to the form changes.
     *
     * Although currently other methods also prevent this to happen, it's still good to explicitly make a copy.
     *  - now the operator property object is immutable, meaning a new property object is construct to replace the old one,
     *      instead of directly mutating the same object reference
     *  - now the formChange event handler checks if the new formData is equal to the current operator data,
     *      which prevents the
     */
    this.currentOperatorInitialData = cloneDeep(operator.operatorProperties);

    // set displayForm to true in the end - first initialize all the data then show the view
    this.displayForm = true;
  }

  /**
   * Handles the highlight / unhighlight events.
   * On highlight -> display the form of the highlighted operator
   * On unhighlight -> hides the form
   */
  public handleHighlightEvents() {
    this.workflowActionService.getJointGraphWrapper().getJointCellHighlightStream()
      .filter(value => value.operatorID !== this.currentOperatorID)
      .map(value => this.workflowActionService.getTexeraGraph().getOperator(value.operatorID))
      .subscribe(
        operator => this.changePropertyEditor(operator)
      );

    this.workflowActionService.getJointGraphWrapper().getJointCellUnhighlightStream()
      .filter(value => value.operatorID === this.currentOperatorID)
      .subscribe(() => this.clearPropertyEditor());
  }

  /**
   * Handles the form change event stream observable,
   *  which corresponds to every event the json schema form library emits.
   *
   * Applies rules that transform the event stream to trigger resonably and less frequently ,
   *  such as debounce time and distince condition.
   *
   * Then modifies the operator property to use the new form data.
   */
  public createOutputFormChangeEventStream(originalSourceFormChangeEvent: Observable<object>): Observable<object> {

    return originalSourceFormChangeEvent
      // set a debounce time to avoid events triggering too often
      //  and to circumvent a bug of the library - each action triggers event twice
      .debounceTime(PropertyEditorComponent.formInputDebounceTime)
      // don't emit the event until the data is changed
      .distinctUntilChanged()
      // don't emit the event if form data is same with current actual data
      // also check for other unlikely circumstances (see below)
      .filter(formData => {
        // check if the current operator ID still exists
        // the user could un-select this operator during debounce time
        if (!this.currentOperatorID) {
          return false;
        }
        // check if the operator still exists
        // the operator could've been deleted during deboucne time
        const operator = this.workflowActionService.getTexeraGraph().getOperator(this.currentOperatorID);
        if (!operator) {
          return false;
        }
        // don't emit event if the form data is equal to actual current property
        // this is to circumvent the library's behavior
        // when the form is initialized, the change event is triggered for the inital data
        // however, the operator property is not changed and shouldn't emit this event
        if (isEqual(formData, operator.operatorProperties)) {
          return false;
        }
        return true;
      })
      // share() because the original observable is a hot observable
      .share();

  }

  /**
   * Generates a form layout used by the json schema form library
   *  to hide the *submit* button.
   * https://github.com/json-schema-form/angular-schema-form/blob/master/docs/index.md#form-definitions
   */
  private static generateFormLayout(): object {
    return [
      '*',
      { type: 'submit', condition: 'false' },
      { type: 'description', condition: 'false' },
    
    
    ];
  }

  // //Map<"string","string">
  // open(property_description:Map<"string","string">) {
  //   console.log('oh noes!')
  //   //this.modalService.open('An inner join is used to return results by combining rows from two or more tables. In its simplest case, where there is no join condition, an inner join would combine all rows from one table with those from another.  If the first table contained three rows, and the second, four, then the final result would contain twelve (3 x 4 = 12) ! The purpose of the join condition is to limit which rows are combined.  In most cases we limit rows to those matching a column.If a person has more than one phone number, then more than one match is made.  From this you can see we may get more rows returned than we have for each person. ');
  //   console.log(this.currentOperatorID);
  //   this.modalService.open(this.property_description);
  // }

  getKeys(property_description:object){
    console.log('in getkeys!')
    console.log(typeof(property_description))


    let arr: Array<String> = [];
    for (let entry in property_description) {
      console.log(entry); 
      //console.log(property_description[entry]); 
      arr.push(entry.bold());
      // arr.push(property_description[entry]);
      arr.push('-----------');
      
   
  }
  
    console.log(arr);
    return arr
  }
}


// @Component {
//   aemfawem
//   template: {
//     <div *ngFor=""> </div>
//   }
// } Name {

//   private Map<> private map;

//   open(new_map) {
//     this.privatemap - new_map
//   }
// }


// @Component({
//   selector: 'ngbd-modal-content',
//   template: `
//     <div class="modal-header">
//       <h4 class="modal-title">Hi there!</h4>
//       <button type="button" class="close" aria-label="Close" (click)="activeModal.dismiss('Cross click')">
//         <span aria-hidden="true">&times;</span>
//       </button>
//     </div>
//     <div class="modal-body">
//       <p>Hello, {{name}}!</p>
//     </div>
//     <div class="modal-footer">
//       <button type="button" class="btn btn-outline-dark" (click)="activeModal.close('Close click')">Close</button>
//     </div>
//   `
// })
// export class NgbdModalContent {
//   constructor(public activeModal: NgbActiveModal) {}
// }