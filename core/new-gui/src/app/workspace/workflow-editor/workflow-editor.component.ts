import { Component, OnInit } from '@angular/core';
declare var $: JQueryStatic;
import * as _ from 'lodash';
import * as backbone from 'backbone';
import * as joint from 'jointjs';

@Component({
  selector: 'texera-workflow-editor',
  templateUrl: './workflow-editor.component.html',
  styleUrls: ['./workflow-editor.component.scss']
})
export class WorkflowEditorComponent implements OnInit {

  constructor() {


  }

  ngOnInit() {
    const graph = new joint.dia.Graph;

    const paper = new joint.dia.Paper({

      el: document.getElementById('workflow-holder'),
      width: 800,
      height: 400,
      gridSize: 1,
      model: graph,
      snapLinks: true,
      linkPinning: false,
      embeddingMode: true,
      highlighting: {
        'default': {
          name: 'stroke'
        },
        'embedding': {
          name: 'addClass'
        }
      },

      validateEmbedding: function (childView, parentView) {

        return parentView.model instanceof joint.shapes.devs.Coupled;
      },

      validateConnection: function (sourceView, sourceMagnet, targetView, targetMagnet) {

        return sourceMagnet !== targetMagnet;
      }
    });

    const connect = function (source, sourcePort, target, targetPort) {

      const link = new joint.shapes.devs.Link({
        source: {
          id: source.id,
          port: sourcePort
        },
        target: {
          id: target.id,
          port: targetPort
        }
      });

      link.addTo(graph).reparent();
    };

    const c1 = new joint.shapes.devs.Coupled({

      position: {
        x: 230,
        y: 50
      },
      size: {
        width: 300,
        height: 300
      }
    });

    c1.set('inPorts', ['in']);
    c1.set('outPorts', ['out 1', 'out 2']);

    const a1 = new joint.shapes.devs.Atomic({

      position: {
        x: 360,
        y: 260
      },
      inPorts: ['xy'],
      outPorts: ['x', 'y']
    });

    const a2 = new joint.shapes.devs.Atomic({

      position: {
        x: 50,
        y: 160
      },
      outPorts: ['out']
    });

    const a3 = new joint.shapes.devs.Atomic({

      position: {
        x: 650,
        y: 50
      },
      size: {
        width: 100,
        height: 300
      },
      inPorts: ['a', 'b']
    });

    graph.addCells([c1, a1, a2, a3]);

    c1.embed(a1);

    connect(a2, 'out', c1, 'in');
    connect(c1, 'in', a1, 'xy');
    connect(a1, 'x', c1, 'out 1');
    connect(a1, 'y', c1, 'out 2');
    connect(c1, 'out 1', a3, 'a');
    connect(c1, 'out 2', a3, 'b');

    /* rounded corners */

    _.each([c1, a1, a2, a3], function (element) {

      element.attr({
        '.body': {
          'rx': 6,
          'ry': 6
        }
      });
    });
  }

}
