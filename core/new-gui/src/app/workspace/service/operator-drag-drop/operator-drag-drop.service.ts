import { Injectable, ElementRef } from '@angular/core';

import { Observable, Subject } from 'rxjs/Rx';

import { CurrentWorkflowService } from '../current-workflow/current-workflow.service';

import * as joint from 'jointjs';

@Injectable()
export class OperatorDragDropService {

  private tempGhostCounter = 0;

  constructor(private currentWorkflowService: CurrentWorkflowService) { }

  createNewOperatorBox() {

    console.log('creating a temp ghost');

    this.tempGhostCounter++;
    const tempGhostID = 'tempGhost-' + this.tempGhostCounter.toString();
    jQuery('body').append('<div id="flyPaper" style="position:fixed;z-index:100;;pointer-event:none;"></div>');

    const tempGhostModel = new joint.dia.Graph();

    const tempGhostPaper = new joint.dia.Paper({
      el: jQuery('#flyPaper'),
      width: 100,
      height: 30,
      model: tempGhostModel,
      gridSize: 1
    });

    console.log('creating a temp graph');

    const operatorUIElement = new joint.shapes.basic.Rect({
      position: { x: 0, y: 0 },
      size: { width: 100, height: 30 },
      attrs: { rect: { fill: 'blue' }, text: { text: 'my box', fill: 'white' } }
    });

    tempGhostModel.addCell(operatorUIElement);

    return jQuery('#flyPaper');
  }

}
