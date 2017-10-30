declare var $: JQueryStatic;
import * as _ from 'lodash';
import * as backbone from 'backbone';
import * as joint from 'jointjs';

export default class WorkflowUIGraph {

    graph = new joint.dia.Graph();

    operatorPredicateMap = new Map<string, joint.dia.Element>();

    constructor() { }

    addOperator(operatorID: string, operatorUIElement?: joint.dia.Element) {
        if (! operatorUIElement) {
            operatorUIElement = new joint.shapes.basic.Rect({
                position: { x: 100, y: 30 },
                size: { width: 100, height: 30 },
                attrs: { rect: { fill: 'blue' }, text: { text: 'my box', fill: 'white' } }
            });
        }

        this.operatorPredicateMap[operatorID] = operatorUIElement;
        this.graph.addCell(operatorUIElement);
    }

}
