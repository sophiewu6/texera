import { Component } from '@angular/core';

import { WorkspaceComponent } from './workspace/workspace.component';

@Component({
  selector: 'texera-root',
  template: `
    <texera-workspace></texera-workspace>
  `,
  styleUrls: ['./app.component.scss']
})
export class AppComponent {
  title = 'texera';
}
