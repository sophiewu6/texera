import { Component, OnInit, EventEmitter, Output } from '@angular/core';

import { OperatorMetadataService } from '../../service/operator-metadata/operator-metadata.service';

import { OperatorLabelComponent } from './operator-label/operator-label.component';
import { FormControl } from '@angular/forms';
import { Observable } from 'rxjs/Observable';
import { startWith } from 'rxjs/operators/startWith';
import { map } from 'rxjs/operators/map';

import * as Fuse from 'fuse.js';
import { MatAutocompleteSelectedEvent } from '@angular/material';
import { WorkflowModelActionService } from '../../service/workflow-graph/model-action/workflow-model-action.service';
import { WorkflowGraphUtilsService } from '../../service/workflow-graph/utils/workflow-graph-utils.service';
import { WorkflowViewEventService } from '../../service/workflow-graph/view-event/workflow-view-event.service';

@Component({
  selector: 'texera-operator-view',
  templateUrl: './operator-view.component.html',
  styleUrls: ['./operator-view.component.scss']
})
export class OperatorViewComponent implements OnInit {

  searchOperatorForm: FormControl = new FormControl();

  filteredOptions = this.searchOperatorForm.valueChanges.pipe(
    startWith<string | OperatorSchema>(''),
    map(value => typeof value === 'string' ? value : value.additionalMetadata.userFriendlyName),
    map(name => this.findOperatorName(name))
  );

  optionId = '';

  currentExpand: string;

  inputMonitor = '';

  public operatorMetadataList: OperatorSchema[] = [];
  public groupNames: string[] = [];
  public operatorGroupMap = new Map<string, OperatorSchema[]>();

  private readonly fuseSearchOptions: Fuse.FuseOptions = {
    tokenize: true,
    matchAllTokens: true,
    threshold: 0.2,
    keys: ['additionalMetadata.userFriendlyName'],
    minMatchCharLength: 2,
    location: 0,
  };
  public fuseSearch = new Fuse(this.operatorMetadataList, this.fuseSearchOptions);

  constructor(
    private operatorMetadataService: OperatorMetadataService,
    private workflowModelActionService: WorkflowModelActionService,
    private workflowGraphUtilsService: WorkflowGraphUtilsService,
    private workflowViewEventService: WorkflowViewEventService,
  ) {
    operatorMetadataService.metadataChanged$.subscribe(value => this.processOperatorMetadata(value));

  }

  ngOnInit() {
  }

  private processOperatorMetadata(operatorMetadata: OperatorSchema[]): void {
    this.operatorMetadataList = operatorMetadata;

    this.groupNames = Array.from(new Set(operatorMetadata.map(schema => schema.additionalMetadata.operatorGroupName)));

    this.operatorGroupMap = new Map<string, OperatorSchema[]>(
      this.groupNames.map(groupName =>
        <[string, OperatorSchema[]]> [groupName, operatorMetadata.filter(x => x.additionalMetadata.operatorGroupName === groupName)]));

    this.fuseSearch = new Fuse(this.operatorMetadataList, this.fuseSearchOptions);
  }

  findOperatorName(query: string): OperatorSchema[] {
    console.log(query);
    const searchResult: OperatorSchema[] = this.fuseSearch.search(query);
    console.log(searchResult);
    return searchResult;
  }

  onAutocompleteOptionSelected(event: MatAutocompleteSelectedEvent) {
    this.inputMonitor = '';
    console.log(event.option);
    const operator = this.workflowGraphUtilsService.getNewOperatorPredicate(event.option.value.operatorType);
    this.workflowModelActionService.addOperator(
      operator, 500, 300);
    this.workflowViewEventService.operatorSelectedInEditor.next({operatorID: operator.operatorID});
  }


  HighlightSelection(option) {
    const operatorLabelID = 'texera-operator-label-'  + option.operatorType;
    if (this.optionId) {
      document.getElementById(this.optionId).style.backgroundColor = '';
    }
    document.getElementById(operatorLabelID).style.backgroundColor = '#ed5281';

    this.optionId = operatorLabelID;
    this.currentExpand = option.additionalMetadata.operatorGroupName.toLowerCase();

  }

  removeSelection() {
    if (this.inputMonitor.length === 0 && this.optionId !== '') {
      document.getElementById(this.optionId).style.backgroundColor = '';
    }
  }

  displayOperatorName(value: OperatorSchema): string {
    if (! value) {
      return '';
    }
    return value.additionalMetadata.userFriendlyName;
  }

}

