import { Component, OnInit, EventEmitter, Output } from '@angular/core';

import { OperatorMetadataService } from '../../service/operator-metadata/operator-metadata.service';

import { OperatorLabelComponent } from './operator-label/operator-label.component';
import { FormControl } from '@angular/forms';
import { Observable } from 'rxjs/Observable';
import { startWith } from 'rxjs/operators/startWith';
import { map } from 'rxjs/operators/map';

@Component({
  selector: 'texera-operator-view',
  templateUrl: './operator-view.component.html',
  styleUrls: ['./operator-view.component.scss']
})
export class OperatorViewComponent implements OnInit {

  operatorCtrl: FormControl = new FormControl();

  filteredOptions: Observable<any[]>;

  public operatorMetadataList: OperatorSchema[] = [];

  optionId: string = '';

  currentExpand: string;

  operatorGroupName: string[] = ['source', 'Search', 'Analysis', 'Split', 'Join', 'Other', 'Write Database', 'standalone'];

  inputMonitor: string = '';


  constructor(private operatorMetadataService: OperatorMetadataService) {
    operatorMetadataService.metadataChanged$.subscribe(x => {
      this.operatorMetadataList = x;
      this.operatorCtrl = new FormControl();
      this.filteredOptions = this.operatorCtrl.valueChanges
        .pipe(
          startWith(''),
          map(option => this.filterOptions(option))
        );
    });
  }

  filterOptions(name: string) {
    return this.operatorMetadataList.filter(option =>
      option.additionalMetadata.userFriendlyName.toLowerCase().indexOf(name.toLowerCase()) !== -1 && name.length > 0);
  }

  ngOnInit() {
    this.operatorMetadataList  = this.operatorMetadataService.getOperatorMetadataList();
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
    if(this.inputMonitor.length===0 && this.optionId != '') {
      document.getElementById(this.optionId).style.backgroundColor = '';
    }
  }

  expandCurrent(group : string) {
    return this.currentExpand === group.toLowerCase();
  }

  transferTitle(group: string) {
    if(group === 'source')
      return 'Sources';
    else if(group === 'standalone')
      return 'View Results';
    else
      return group;
  }

}

