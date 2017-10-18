import { Component, OnInit } from '@angular/core';

import { OperatorMetadataService } from '../service/operator-metadata/operator-metadata.service';

@Component({
  selector: 'texera-operator-view',
  templateUrl: './operator-view.component.html',
  styleUrls: ['./operator-view.component.scss']
})
export class OperatorViewComponent implements OnInit {

  public test: any;

  constructor(private operatorMetadataService: OperatorMetadataService) { }

  ngOnInit() {
    this.operatorMetadataService.getOperatorMetadataMap().subscribe(result => this.test = result.get('ScanSource').userFriendlyName);
  }

}
