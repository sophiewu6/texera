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

  optionId: string;


  constructor(private operatorMetadataService: OperatorMetadataService) {
    operatorMetadataService.metadataChanged$.subscribe(x => {
      this.operatorMetadataList = x;
      this.operatorCtrl = new FormControl();
      console.log('Filter Options ? ');
      console.log(this.filteredOptions);
      this.filteredOptions = this.operatorCtrl.valueChanges
        .pipe(
          startWith(''),
          map(option => this.filterOptions(option))
        );
      console.log('Current filter op ');
      console.log(this.filteredOptions);
    });
  }

  filterOptions(name: string) {
    return this.operatorMetadataList.filter(option =>
      option.additionalMetadata.userFriendlyName.toLowerCase().indexOf(name.toLowerCase()) !== -1);
  }

  ngOnInit() {
    this.operatorMetadataList  = this.operatorMetadataService.getOperatorMetadataList();
  }


  HighlightSelection(option) {
    console.log(option.operatorGroupName);
    const operatorLabelID = 'texera-operator-label-'  + option.operatorType;
    if (this.optionId) {
      document.getElementById(this.optionId).style.color = 'black';
    }
    document.getElementById(operatorLabelID).style.color = 'red';

    this.optionId = operatorLabelID;

  }



}

