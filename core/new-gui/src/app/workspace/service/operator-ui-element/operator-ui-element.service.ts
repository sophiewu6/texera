import { Injectable } from '@angular/core';

import * as joint from 'jointjs';
import { OperatorMetadataService } from '../operator-metadata/operator-metadata.service';

@Injectable()
export class OperatorUIElementService {

  private operatorMetadata: OperatorSchema[] = [];

  constructor(private operatorMetadataService: OperatorMetadataService) {
    this.operatorMetadataService.metadataChanged$.subscribe(
      value => this.operatorMetadata = value
    );

  }

  getOperatorUIElement(operatorType: string, operatorID: string): joint.dia.Element {
    const operatorSchema = this.operatorMetadata.find(schema => schema.operatorType === operatorType);

    joint.shapes.devs['TexeraModel'] = joint.shapes.devs.Model.extend({
      type: 'devs.TexeraModel',
      markup : '<g class="element-node">' +
             '<rect class="body" stroke-width="2" stroke="blue" rx="5px" ry="5px"></rect>' +
             '<svg class="delete-button" fill="#000000" height="24" viewBox="0 0 24 24" width="24" xmlns="http://www.w3.org/2000/svg">' +
                 '<path d="M0 0h24v24H0z" fill="none" pointer-events="visible" />' +
                 // tslint:disable-next-line:max-line-length
                 '<path d="M14.59 8L12 10.59 9.41 8 8 9.41 10.59 12 8 14.59 9.41 16 12 13.41 14.59 16 16 14.59 13.41 12 16 9.41 14.59 8zM12 2C6.47 2 2 6.47 2 12s4.47 10 10 10 10-4.47 10-10S17.53 2 12 2zm0 18c-4.41 0-8-3.59-8-8s3.59-8 8-8 8 3.59 8 8-3.59 8-8 8z"/>' +
             '</svg>' +
            '<text>' +
            '</text>' +
          '<g class="inPorts"/>' +
          '<g class="outPorts"/>' +
        '</g>'
    });

    const operatorElement = new joint.shapes.devs['TexeraModel']({
      id: operatorID,
      position: { x: 0, y: 0 },
      size: { width: 120, height: 50 },
      attrs: {
        'rect': { fill: '#FFFFFF' , 'follow-scale': true, stroke: '#CFCFCF', 'stroke-width': '2' },
        'text': { text: operatorType, fill: 'black', 'font-size': '12px',
        'ref-x': 0.5, 'ref-y' : 0.5, ref: 'rect',  'y-alignment': 'middle', 'x-alignment': 'middle' },
        '.delete-button' : { x : 120, y : -15, cursor : 'pointer',
        fill : 'red', event: 'element:delete'}
      }
    });

    for (let i = 0; i < operatorSchema.additionalMetadata.numInputPorts; i++) {
      operatorElement.addInPort(`in${i}`);
    }
    // set output ports
    for (let i = 0; i < operatorSchema.additionalMetadata.numOutputPorts; i++) {
      operatorElement.addOutPort(`out${i}`);
    }
    return operatorElement;
  }

}
