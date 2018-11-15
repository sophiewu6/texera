import { NgModule } from '@angular/core';
import { Routes, RouterModule } from '@angular/router';

import { WorkspaceComponent } from './workspace/component/workspace.component';

import { DashboardComponent } from './dashboard/component/dashboard.component';

const routes: Routes = [
  {
    path : '',
    component : WorkspaceComponent
  },
  {
    path : 'Dashboard',
    component : DashboardComponent,
    children : [
      {
        path : 'SavedProject',
        redirectTo: ''
      },
      {
        path : 'UserDictionary',
        redirectTo: ''
      }
    ]
  }
];

@NgModule({
  imports: [RouterModule.forRoot(routes)],
  exports: [RouterModule]
})
export class AppRoutingModule { }
