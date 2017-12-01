import { Injectable } from '@angular/core';

import * as joint from 'jointjs';
import { OperatorMetadataService } from '../operator-metadata/operator-metadata.service';

@Injectable()
export class OperatorUIElementService {

  constructor(private operatorMetadataService: OperatorMetadataService) { }

  getOperatorUIElement(operatorType: string): joint.dia.Element {
    const operatorSchema = this.operatorMetadataService.getOperatorMetadata(operatorType);
    const operatorElement = new joint.shapes.devs.Model({
      position: { x: 0, y: 0 },
      size: { width: 100, height: 30 },
      attrs: { rect: { fill: 'grey' }, text: { text: operatorType, fill: 'black' } }
    });
    // set input ports
    for (let i = 0; i < operatorSchema.numInputPorts; i++) {
      operatorElement.addInPort(`in${i}`);
    }
    // set output ports
    for (let i = 0; i < operatorSchema.numOutputPorts; i++) {
      operatorElement.addOutPort(`out${i}`);
    }
    return operatorElement;
  }

}
