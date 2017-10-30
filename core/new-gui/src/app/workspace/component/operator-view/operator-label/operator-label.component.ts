import { Component, OnInit, Input, AfterViewInit } from '@angular/core';

declare var jQuery: JQueryStatic;
import OperatorSchema from '../../../model/operator-schema';
import { OperatorDragDropService } from '../../../service/operator-drag-drop/operator-drag-drop.service';

@Component({
  selector: 'texera-operator-label',
  templateUrl: './operator-label.component.html',
  styleUrls: ['./operator-label.component.scss']
})
export class OperatorLabelComponent implements OnInit, AfterViewInit {

  @Input() operator: OperatorSchema;

  operatorLabelID: string;
  operatorGhostID: string;

  isDragging = false;

  constructor(private operatorDragDropService: OperatorDragDropService) {
  }

  ngOnInit() {
    this.operatorLabelID = 'texera-operator-label-' + this.operator.operatorType;
    this.operatorGhostID = 'texera-operator-ghost-' + this.operator.operatorType;
  }

  ngAfterViewInit() {
    this.initializeDragDrop();
  }

  private initializeDragDrop() {
    jQuery('#' + this.operatorLabelID).draggable(
      {
        helper: () => this.createDragHelper(),
        start: (event, ui) => this.dragStarted(event, ui),
        stop: () => this.dragEnded()
      }
    );
  }

  private createDragHelper() {
    return this.operatorDragDropService.createNewOperatorBox();
  }

  private dragStarted(event: Event, ui: JQueryUI.DraggableEventUIParams) {
    console.log('drag started');
    this.isDragging = true;
  }

  private dragEnded() {
    console.log('drag ended');
    console.log(this.isDragging);
    this.isDragging = false;
    console.log(this.isDragging);
  }

}
