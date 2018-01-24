import { Component, OnInit, ChangeDetectorRef } from '@angular/core';
import { DataSource } from '@angular/cdk/table';

import { ExecuteWorkflowService } from '../../service/execute-workflow/execute-workflow.service';
import { Observable } from 'rxjs/Observable';

/* tslint:disable:no-inferrable-types */
@Component({
  selector: 'texera-result-view',
  templateUrl: './result-view.component.html',
  styleUrls: ['./result-view.component.scss']
})
export class ResultViewComponent implements OnInit {

  showMessage: boolean = false;
  message: string = '';

  test = [1,2,3];
  currentColumns: TableColumn[] = undefined;
  currentDisplayColumns: string[] = undefined;
  currentDataSource: ResultDataSource = undefined;

  /** Column definitions in order */
  // displayedColumns = this.columns.map(x => x.columnDef);

  constructor(private executeWorkflowService: ExecuteWorkflowService, private changeDetectorRef: ChangeDetectorRef) {
    executeWorkflowService.executeFinished$.subscribe(
      value => this.handleResultData(value),
      error => this.handleError(error)
    );
  }

  private handleResultData(resultData: any): void {
    if (! resultData || resultData.length === 0) {
      this.handleError('No result');
      return;
    }
    this.showMessage = false;
    // generate columnDef from first row
    console.log(resultData);
    this.currentDisplayColumns = Object.keys(resultData[0]).filter(x => x !== '_id');

    // this.currentDisplayColumns = ['test','position', 'name', 'weight', 'symbol'];

    this.currentColumns = this.generateColumns(this.currentDisplayColumns);
    this.currentDataSource = new ResultDataSource(resultData);
    console.log(this.currentDisplayColumns);
    console.log(this.currentDataSource);
  }

  private generateColumns(columnNames: string[]): TableColumn[] {
    const columns: TableColumn[] = [];
    columnNames.forEach(col => columns.push(new TableColumn(col, col, (row) => `${row[col]}`)));
    console.log(columns);
    return columns;
  }

  private handleError(error: any): void {
    this.showMessage = true;
    this.message = JSON.stringify(error);
  }

  ngOnInit() {
  }

}

export class TableColumn {
  constructor(
    public columnDef: string,
    public header: string,
    public cell: (row: any) => any
  ) { }
}


export class ResultDataSource extends DataSource<Object> {

  constructor(private resultData: Object[]) {
    super();
  }

  connect(): Observable<Object[]> {
    return Observable.of(this.resultData);
  }

  disconnect() {
  }

}



export interface Element {
  name: string;
  position: number;
  weight: number;
  symbol: string;
  test: string;
}

const ELEMENT_DATA: Element[] = [
  {test: "HEHEHEHEHEHEHEHEHEHEHEHEHEHEHEHEHEHEHEHEHEHEHEHEHEHEHEHEHEHE", position: 4, name: 'BerylliumBerylliumBerylliumBerylliumBerylliumBerylliumBerylliumBerylliumBerylliumBerylliumBerylliumBerylliumBerylliumBerylliumBerylliumBerylliumBerylliumBerylliumBerylliumBerylliumBerylliumBerylliumBerylliumBerylliumBerylliumBerylliumBerylliumBerylliumBerylliumBerylliumBerylliumBerylliumBeryllium', weight: 9.0122, symbol: 'Be'},
  {test: "HEHEHEHEHEHE", position: 9, name: 'Fluorine', weight: 18.9984, symbol: 'BerylliumBerylliumBerylliumBerylliumBerylliumBerylliumBerylliumBerylliumBerylliumBerylliumBerylliumBerylliumBerylliumBerylliumBerylliumBerylliumBerylliumBerylliumBerylliumBerylliumBerylliumBerylliumBerylliumBerylliumBerylliumBerylliumBerylliumBerylliumBerylliumBerylliumBerylliumBerylliumBerylliumBerylliumBerylliumBerylliumBerylliumBerylliumBerylliumBerylliumBerylliumBerylliumBerylliumBerylliumBerylliumBerylliumBerylliumBerylliumBerylliumBerylliumBerylliumBerylliumBerylliumBerylliumBerylliumBerylliumBerylliumBerylliumBerylliumBerylliumBerylliumBerylliumBerylliumBerylliumBerylliumBerylliumBerylliumBerylliumBerylliumBerylliumBerylliumBerylliumBerylliumBerylliumBerylliumBerylliumBerylliumBerylliumBerylliumBerylliumBerylliumBerylliumBerylliumBerylliumBerylliumBerylliumBerylliumBerylliumBerylliumBerylliumBerylliumBerylliumBerylliumBerylliumBerylliumBerylliumBerylliumBerylliumBeryllium'},
  {test: "HEHEHEHEHEHE", position: 12, name: 'BerylliumBerylliumBerylliumBerylliumBerylliumBerylliumBerylliumBerylliumBerylliumBerylliumBerylliumBerylliumBerylliumBerylliumBerylliumBerylliumBerylliumBerylliumBerylliumBerylliumBerylliumBerylliumBerylliumBerylliumBerylliumBerylliumBerylliumBerylliumBerylliumBerylliumBerylliumBerylliumBerylliumBerylliumBerylliumBerylliumBerylliumBerylliumBerylliumBerylliumBerylliumBerylliumBerylliumBerylliumBerylliumBerylliumBerylliumBerylliumBerylliumBerylliumBerylliumBerylliumBerylliumBerylliumBerylliumBerylliumBerylliumBerylliumBerylliumBerylliumBerylliumBerylliumBerylliumBerylliumBerylliumBeryllium', weight: 24.305, symbol: 'Mg'}
];
