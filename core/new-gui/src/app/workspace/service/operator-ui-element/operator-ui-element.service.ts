import { Injectable } from '@angular/core';

import * as joint from 'jointjs';

@Injectable()
export class OperatorUIElementService {

  constructor() { }

  getOperatorUIElement(operatorType: string): joint.dia.Element {
    const operatorRect = new joint.shapes.basic.Rect({
      position: { x: 0, y: 0 },
      size: { width: 100, height: 30 },
      attrs: { rect: { fill: 'grey' }, text: { text: operatorType, fill: 'black' } }
    });
    return operatorRect;
  }

}
