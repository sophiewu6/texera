import { BrowserModule } from '@angular/platform-browser';
import { NgModule } from '@angular/core';
import { HttpModule } from '@angular/http';
import { Observable } from 'rxjs/Rx';

import { FlexLayoutModule } from '@angular/flex-layout';

import { AppRoutingModule } from './app-routing.module';
import { AppComponent } from './app.component';
import { WorkspaceComponent } from './workspace/workspace.component';
import { WorkflowEditorComponent } from './workspace/workflow-editor/workflow-editor.component';
import { NavigationComponent } from './workspace/navigation/navigation.component';
import { PropertyEditorComponent } from './workspace/property-editor/property-editor.component';
import { OperatorViewComponent } from './workspace/operator-view/operator-view.component';
import { ResultViewComponent } from './workspace/result-view/result-view.component';

@NgModule({
  declarations: [
    AppComponent,
    WorkspaceComponent,
    WorkflowEditorComponent,
    NavigationComponent,
    PropertyEditorComponent,
    OperatorViewComponent,
    ResultViewComponent
  ],
  imports: [
    BrowserModule,
    AppRoutingModule,
    HttpModule,
    FlexLayoutModule
  ],
  providers: [],
  bootstrap: [AppComponent]
})
export class AppModule { }
