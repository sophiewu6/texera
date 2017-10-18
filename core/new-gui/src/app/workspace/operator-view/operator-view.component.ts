import { Component, OnInit } from '@angular/core';

import OperatorSchema from '../model/operator-schema';
import { OperatorMetadataService } from '../service/operator-metadata/operator-metadata.service';

@Component({
  selector: 'texera-operator-view',
  templateUrl: './operator-view.component.html',
  styleUrls: ['./operator-view.component.scss']
})
export class OperatorViewComponent implements OnInit {

  public operatorMetadataList: OperatorSchema[] = [];

  constructor(private operatorMetadataService: OperatorMetadataService) { }

  ngOnInit() {
    this.operatorMetadataService.getOperatorMetadataList().subscribe(
      result => this.operatorMetadataList = result);
  }

}
