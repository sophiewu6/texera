import { OperatorLink } from './../../../types/workflow-common.interface';

import { WorkflowGraph } from './workflow-graph';
import { JointGraphWrapper } from './joint-graph-wrapper';
import { timer } from 'rxjs/observable/timer';

/**
 * SyncTexeraModel subscribes to the graph change events from JointJS,
 *  then sync the changes to the TexeraGraph:
 *    - delete operator
 *    - link events: link add/delete/change
 *    - operator position change
 *
 * For details of handling each JointJS event type, see the comments below for each function.
 *
 * For an overview of the services in WorkflowGraphModule, see workflow-graph-design.md
 *
 */
export class SyncTexeraModel {

  private syncJointEvent: boolean = true;

  constructor(
    private texeraGraph: WorkflowGraph,
    private jointGraphWrapper: JointGraphWrapper
  ) {

    this.handleJointOperatorDelete();
    this.handleJointLinkEvents();
    this.handleJointOperatorMove();
  }

  public turnOnSyncJointEvent(): void {
    this.syncJointEvent = true;
  }

  public turnOffSyncJointEvent(): void {
    this.syncJointEvent = false;
  }


  /**
   * Handles JointJS operator element delete events:
   *  deletes the corresponding operator in Texera workflow graph.
   *
   * Deletion of an operator will also cause its connected links to be deleted as well,
   *  JointJS automatically hanldes this logic,
   *  therefore we don't handle it to avoid inconsistency (deleting already deleted link).
   *
   * When an operator is deleted, JointJS will trigger the corresponding
   *  link delete events and cause texera link to be deleted.
   */
  private handleJointOperatorDelete(): void {
    this.jointGraphWrapper.getJointOperatorCellDeleteStream()
      .filter(() => this.syncJointEvent)
      .map(element => element.id.toString())
      .subscribe(elementID => this.texeraGraph.deleteOperator(elementID));
  }

  /**
   * Handles JointJS link events:
   * JointJS link events reflect the changes to the link View in the UI.
   * Workflow link requires the link to have both source and target port to be considered valid.
   *
   * Cases where JointJS and Texera link have different semantics:
   *  - When the user drags the link from one port, but not yet to connect to another port,
   *      the link is added in the semantic of JointJS, but not in the semantic of Texera Workflow graph.
   *  - When an invalid link that is not connected to a port disappears,
   *      the link delete event is trigger by JointJS, but the link never existed from Texera's perspective.
   *  - When the user drags and detaches the end of a valid link, the link is disconnected from the target port,
   *      the link change event (not delete) is triggered by JointJS, but the link should be deleted from Texera's graph.
   *  - When the user attaches the end of the link to a target port,
   *      the link change event (not add) is triggered by JointJS, but it should be added to the Texera Graph.
   *  - When the user drags the link around, the link change event will be trigger continuously,
   *      when the target being a changing coordinate. But this event should not have any effect on the Texera Graph.
   *
   * To address the disparity of the semantics, the linkAdded / linkDeleted / linkChanged events need to be handled carefully.
   */
  private handleJointLinkEvents(): void {
    /**
     * on link cell add:
     * we need to check if the link is a valid link in Texera's semantic (has both source and target port)
     *  and only add valid links to the graph
     */
    this.jointGraphWrapper.getJointLinkCellAddStream()
      .filter(() => this.syncJointEvent)
      .filter(link => SyncTexeraModel.isValidJointLink(link))
      .map(link => SyncTexeraModel.getOperatorLink(link))
      .subscribe(link => this.texeraGraph.addLink(link));

    /**
     * on link cell delete:
     * we need to first check if the link is a valid link
     *  then delete the link by the link ID
     */
    this.jointGraphWrapper.getJointLinkCellDeleteStream()
      .filter(() => this.syncJointEvent)
      .filter(link => SyncTexeraModel.isValidJointLink(link))
      .subscribe(link => this.texeraGraph.deleteLinkWithID(link.id.toString()));


    /**
     * on link cell change:
     * link cell change could cause deletion of a link or addition of a link, or simply no effect
     * TODO: finish this documentation
     */
    this.jointGraphWrapper.getJointLinkCellChangeStream()
      .filter(() => this.syncJointEvent)
      // we intentially want the side effect (delete the link) to happen **before** other operations in the chain
      .do((link) => {
        const linkID = link.id.toString();
        if (this.texeraGraph.hasLinkWithID(linkID)) { this.texeraGraph.deleteLinkWithID(linkID); }
      })
      .filter(link => SyncTexeraModel.isValidJointLink(link))
      .map(link => SyncTexeraModel.getOperatorLink(link))
      .subscribe(link => {
        this.texeraGraph.addLink(link);
      });
  }

  /**
   *
   * Handles JointJS operator operator move event stream,
   *  truncate the event stream and omit some events,
   *  to make the operator move only fire after the user stops moving for some time,
   *  instead of firing at each little move step.
   *
   * Suppose we have operators with ID a, b, c, etc..
   * each time the operator is moved, it emits events a1, a2, a3, or b1, b2, b3, etc..
   *
   * Suppose we have the following scenario:
   * The user moves operator a around, then the user *quickly* moves operator b around.
   * The source event stream will be like:
   * --a1--a2--a3--b1--b2--b3--
   *
   * If we naively set a debounceTime on the whole event stream,
   *  if interval between `a3` and `b1` is less than the debounceTime,
   *  then only the final `b3` event will be emitted,
   *  as if operator `a` has never been moved, as shown in the following diagram
   *
   * source: --a1--a2--a3--b1--b2--b3--
   *           debounceTime(longTime)
   * target:  ---------------------------------b3--
   *
   * We want to set the debounceTime high to avoid producing too many operator move events,
   *  however, setting it too high will cause the problem stated above.
   *
   * Solution:
   *  the event stream is groupBy operatorID,
   *  each operatorID has its own sub-observable,
   *  apply debounceTime on each operatorID's observable
   *
   * source:    --a1--a2--a3--b1--b2--b3-- (observable of events)
   *                groupBy(operatorID): a new observable is created for each operatorID
   * groupped:  --a1----------b1---------- (observable of observables)
   *               \           \
   *                a2          b2
   *                 \           \
   *                  a3          b3
   *                   \           \
   *              map(obs => obs.debounceTime): apply debounceTime to each sub-observable
   * debounced: --------------------------
   *               \           \
   *                \           \    (emit after the debouncetime)
   *                 a3          b3
   *                  \           \
   *              mergeAll(): merge all the sub-observables back together
   * merged:    ---------------------a3----------b3--
   *
   * This will get the expected behavoir: each operator's move event stream is treated separately
   *
   */
  private handleJointOperatorMove(): void {
    this.jointGraphWrapper.getJointOperatorMoveStream()
      .filter(() => this.syncJointEvent)
      .groupBy(value => value.operatorID)
      .map(observableEachOperator => observableEachOperator.debounceTime(800))
      .mergeAll()
      .subscribe(value =>
        this.texeraGraph.setOperatorPosition(value.operatorID, value.position)
      );
  }

  /**
   * Transforms a JointJS link (joint.dia.Link) to a Texera Link object
   * The JointJS link must be valid, otherwise an error will be thrown.
   * @param jointLink
   */
  static getOperatorLink(jointLink: joint.dia.Link): OperatorLink {

    type jointLinkEventType = { id: string, port: string } | null | undefined;

    // the link should be a valid link (both source and target are connected to an operator)
    // isValidLink function is not reused because of Typescript strict null checking
    const jointSourceElement: jointLinkEventType = jointLink.attributes.source;
    const jointTargetElement: jointLinkEventType = jointLink.attributes.target;

    if (!jointSourceElement) {
      throw new Error(`Invalid JointJS Link: no source element`);
    }

    if (!jointTargetElement) {
      throw new Error(`Invalid JointJS Link: no target element`);
    }

    return {
      linkID: jointLink.id.toString(),
      source: {
        operatorID: jointSourceElement.id,
        portID: jointSourceElement.port
      },
      target: {
        operatorID: jointTargetElement.id,
        portID: jointTargetElement.port
      }
    };
  }

  /**
   * Determines if a jointJS link is valid (both ends are connected to a port of  port).
   * If a JointJS link's target is still a point (not connected), it's not considered a valid link.
   * @param jointLink
   */
  static isValidJointLink(jointLink: joint.dia.Link): boolean {
    return jointLink && jointLink.attributes &&
      jointLink.attributes.source && jointLink.attributes.target &&
      jointLink.attributes.source.id && jointLink.attributes.source.port &&
      jointLink.attributes.target.id && jointLink.attributes.target.port;
  }


}


