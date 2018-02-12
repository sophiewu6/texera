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

  currentColumns: TableColumn[] = undefined;
  currentDisplayColumns: string[] = undefined;
  currentDataSource: ResultDataSource = undefined;

  /** Column definitions in order */
  // displayedColumns = this.columns.map(x => x.columnDef);

  constructor(private executeWorkflowService: ExecuteWorkflowService, private changeDetectorRef: ChangeDetectorRef) {
    this.executeWorkflowService.executeFinished$.subscribe(
      value => this.handleResultData(value),
    );
  }

  private handleResultData(response: any): void {
    console.log('view result compoenent, ');
    console.log(response);
    if (response.code === 0) {
      console.log('show success data');
      this.showMessage = false;
      // generate columnDef from first row
      const resultData: Object[] = response.result;
      this.currentDisplayColumns = Object.keys(resultData[0]).filter(x => x !== '_id');
      this.currentColumns = this.generateColumns(this.currentDisplayColumns);
      this.currentDataSource = new ResultDataSource(resultData);
      console.log(this.currentDisplayColumns);
    } else {
      console.log('show fail message');
      this.showMessage = true;
      this.message = JSON.stringify(response.message);
    }
  }

  private generateColumns(columnNames: string[]): TableColumn[] {
    const columns: TableColumn[] = [];
    columnNames.forEach(col => columns.push(new TableColumn(col, col, (row) => `${row[col]}`)));
    console.log(columns);
    return columns;
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

