import { Component, OnInit, Input, AfterViewInit } from '@angular/core';

declare var jQuery: JQueryStatic;
import { OperatorSchema } from '../../../model/operator-schema';
import { OperatorDragDropService } from '../../../service/operator-drag-drop/operator-drag-drop.service';

@Component({
  selector: 'texera-operator-label',
  templateUrl: './operator-label.component.html',
  styleUrls: ['./operator-label.component.scss']
})
export class OperatorLabelComponent implements OnInit, AfterViewInit {

  @Input() operator: OperatorSchema;

  operatorLabelID: string;

  constructor(private operatorDragDropService: OperatorDragDropService) {
  }

  ngOnInit() {
    this.operatorLabelID = 'texera-operator-label-' + this.operator.operatorType;
  }

  ngAfterViewInit() {
    this.initializeDragDrop();
  }

  private initializeDragDrop() {
    jQuery('#' + this.operatorLabelID).draggable(
      {
        helper: () => this.createDragHelper()
      }
    );
  }

  private createDragHelper() {
    return this.operatorDragDropService.createNewOperatorUIElement(this.operator.operatorType);
  }

}
