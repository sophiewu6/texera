import { mockScanPredicate, mockResultPredicate, mockScanResultLink } from './../workflow-graph/model/mock-workflow-data';
import { JointUIService } from './../joint-ui/joint-ui.service';
import { WorkflowActionService } from './../workflow-graph/model/workflow-action.service';
import { TestBed, inject } from '@angular/core/testing';

import { UndoRedoService } from './undo-redo.service';
import { OperatorMetadataService } from '../operator-metadata/operator-metadata.service';
import { StubOperatorMetadataService } from '../operator-metadata/stub-operator-metadata.service';

fdescribe('UndoRedoService', () => {

  let workflowActionService: WorkflowActionService;
  let undoRedoService: UndoRedoService;

  beforeEach(() => {
    TestBed.configureTestingModule({
      providers: [
        UndoRedoService,
        WorkflowActionService,
        JointUIService,
        { provide: OperatorMetadataService, useClass: StubOperatorMetadataService }
      ]
    });
    workflowActionService = TestBed.get(WorkflowActionService);
    undoRedoService = TestBed.get(UndoRedoService);
  });

  it('should be created', inject([UndoRedoService], (service: UndoRedoService) => {
    expect(service).toBeTruthy();
  }));

  it('should handle add operator action correctly', () => {
    // add an operator
    workflowActionService.addOperator(mockScanPredicate);

    // check undo and redo stack size
    expect(undoRedoService.getUndoStack().size()).toEqual(1);
    expect(undoRedoService.getRedoStack().size()).toEqual(0);

    // undo that operation
    undoRedoService.undo();

    // check actual graph
    expect(workflowActionService.getTexeraGraph().hasOperator(mockScanPredicate.operatorID)).toBeFalsy();
    // check undo and redo stack size
    expect(undoRedoService.getUndoStack().size()).toEqual(0);
    expect(undoRedoService.getRedoStack().size()).toEqual(1);

    // try to redo the operation
    undoRedoService.redo();

    // check actual graph
    expect(workflowActionService.getTexeraGraph().hasOperator(mockScanPredicate.operatorID)).toBeTruthy();
    expect(workflowActionService.getTexeraGraph().getOperator(mockScanPredicate.operatorID)).toEqual(mockScanPredicate);

    // cehck undo and redo stack size
    expect(undoRedoService.getUndoStack().size()).toEqual(1);
    expect(undoRedoService.getRedoStack().size()).toEqual(0);

  });

  it('should handle delete operator action correctly', () => {
    // add an operator first
    workflowActionService.addOperator(mockScanPredicate);

    // then delete the operator
    workflowActionService.deleteOperator(mockScanPredicate.operatorID);

    // undo the delete operation
    undoRedoService.undo();

    // check actual graph
    expect(workflowActionService.getTexeraGraph().hasOperator(mockScanPredicate.operatorID)).toBeTruthy();
    // check undo and redo stack size
    expect(undoRedoService.getUndoStack().size()).toEqual(1);
    expect(undoRedoService.getRedoStack().size()).toEqual(1);

    // try to redo the operation
    undoRedoService.redo();

    // check actual graph
    expect(workflowActionService.getTexeraGraph().hasOperator(mockScanPredicate.operatorID)).toBeFalsy();

    // cehck undo and redo stack size
    expect(undoRedoService.getUndoStack().size()).toEqual(2);
    expect(undoRedoService.getRedoStack().size()).toEqual(0);

  });

  it('should handle add link action correctly', () => {
    // add two operators
    workflowActionService.addOperator(mockScanPredicate);
    workflowActionService.addOperator(mockResultPredicate);

    // add a link
    workflowActionService.addLink(mockScanResultLink);

    // undo that operation
    undoRedoService.undo();

    // check actual graph
    expect(workflowActionService.getTexeraGraph().hasLink(
      mockScanResultLink.source, mockScanResultLink.target
    )).toBeFalsy();
    // check undo and redo stack size
    expect(undoRedoService.getUndoStack().size()).toEqual(2);
    expect(undoRedoService.getRedoStack().size()).toEqual(1);

    // try to redo the operation
    undoRedoService.redo();

    // check actual graph
    expect(workflowActionService.getTexeraGraph().hasLink(
      mockScanResultLink.source, mockScanResultLink.target
    )).toBeTruthy();

    // cehck undo and redo stack size
    expect(undoRedoService.getUndoStack().size()).toEqual(3);
    expect(undoRedoService.getRedoStack().size()).toEqual(0);

  });

  it('should handle delete link action correctly', () => {
    // add two operators
    workflowActionService.addOperator(mockScanPredicate);
    workflowActionService.addOperator(mockResultPredicate);
    // add a link
    workflowActionService.addLink(mockScanResultLink);

    // delete the link
    workflowActionService.deleteLink(mockScanResultLink.source, mockScanResultLink.target);

    // undo that operation
    undoRedoService.undo();

    // check actual graph
    expect(workflowActionService.getTexeraGraph().hasLink(
      mockScanResultLink.source, mockScanResultLink.target
    )).toBeTruthy();
    // check undo and redo stack size
    expect(undoRedoService.getUndoStack().size()).toEqual(3);
    expect(undoRedoService.getRedoStack().size()).toEqual(1);

    // try to redo the operation
    undoRedoService.redo();

    // check actual graph
    expect(workflowActionService.getTexeraGraph().hasLink(
      mockScanResultLink.source, mockScanResultLink.target
    )).toBeFalsy();

    // cehck undo and redo stack size
    expect(undoRedoService.getUndoStack().size()).toEqual(4);
    expect(undoRedoService.getRedoStack().size()).toEqual(0);

  });

  it('should handle add operator action correctly', () => {
    // add an operator
    workflowActionService.addOperator(mockScanPredicate);

    // check undo and redo stack size
    expect(undoRedoService.getUndoStack().size()).toEqual(1);
    expect(undoRedoService.getRedoStack().size()).toEqual(0);

    // undo that operation
    undoRedoService.undo();

    // check actual graph
    expect(workflowActionService.getTexeraGraph().hasOperator(mockScanPredicate.operatorID)).toBeFalsy();
    // check undo and redo stack size
    expect(undoRedoService.getUndoStack().size()).toEqual(0);
    expect(undoRedoService.getRedoStack().size()).toEqual(1);

    // try to redo the operation
    undoRedoService.redo();

    // check actual graph
    expect(workflowActionService.getTexeraGraph().hasOperator(mockScanPredicate.operatorID)).toBeTruthy();
    expect(workflowActionService.getTexeraGraph().getOperator(mockScanPredicate.operatorID)).toEqual(mockScanPredicate);

    // cehck undo and redo stack size
    expect(undoRedoService.getUndoStack().size()).toEqual(1);
    expect(undoRedoService.getRedoStack().size()).toEqual(0);

  });


});
