import { UndoRedoService } from './../../service/undo-redo/undo-redo.service';
import { Component, OnInit } from '@angular/core';

@Component({
  selector: 'texera-navigation',
  templateUrl: './navigation.component.html',
  styleUrls: ['./navigation.component.scss']
})
export class NavigationComponent implements OnInit {

  constructor(
    public undoRedoService: UndoRedoService
  ) { }

  ngOnInit() {
  }

  public undoPressed(): void {
    this.undoRedoService.undo();
  }

  public redoPressed(): void {
    this.undoRedoService.redo();
  }

}
