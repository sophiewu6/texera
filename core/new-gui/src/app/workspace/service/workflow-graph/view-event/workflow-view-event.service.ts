import { Injectable } from '@angular/core';
import { Observable, Subject } from 'rxjs/Rx';
import '../../../common/rxjs-operators.ts';

@Injectable()
export class WorkflowViewEventService {

  public operatorSelectedInEditor = new Subject<string>();

  public deleteOperatorClickedInEditor = new Subject<string>();

  public deleteLinkClickedInEditor = new Subject<string>();

  public pointerDownOnBlankInEditor = new Subject<{ event: Event, x: number, y: number }>();

  public linkConnectedInEditor = new Subject<{
    linkView: joint.dia.LinkView,
    evt: Event,
    elementViewConnected: joint.dia.ElementView,
    magnet: SVGElement,
    arrowhead: any
  }>();

  public linkDisconnectedInEditor = new Subject<{
    linkView: joint.dia.LinkView,
    evt: Event,
    elementViewDisconnected: joint.dia.ElementView,
    magnet: SVGElement,
    arrowhead: any
  }>();

  constructor() { }

}
