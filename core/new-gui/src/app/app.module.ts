import { BrowserModule } from '@angular/platform-browser';
import { NgModule } from '@angular/core';
import { HttpClientModule } from '@angular/common/http';

import { AppRoutingModule } from './app-routing.module';

import { CustomNgMaterialModule } from './common/custom-ng-material.module';
import { BrowserAnimationsModule } from '@angular/platform-browser/animations';
import { NgbModule } from '@ng-bootstrap/ng-bootstrap';
import { RouterModule } from '@angular/router';
import { TourNgBootstrapModule } from 'ngx-tour-ng-bootstrap';


import { MaterialDesignFrameworkModule } from 'angular6-json-schema-form';


import { AppComponent } from './app.component';
import { WorkspaceComponent } from './workspace/component/workspace.component';
import { NavigationComponent } from './workspace/component/navigation/navigation.component';
import { OperatorPanelComponent } from './workspace/component/operator-panel/operator-panel.component';
import { PropertyEditorComponent } from './workspace/component/property-editor/property-editor.component';
import { WorkflowEditorComponent } from './workspace/component/workflow-editor/workflow-editor.component';
import { ResultPanelComponent, NgbModalComponent } from './workspace/component/result-panel/result-panel.component';
import { OperatorLabelComponent } from './workspace/component/operator-panel/operator-label/operator-label.component';
import { ProductTourComponent } from './workspace/component/product-tour/product-tour.component';
import { ResultPanelToggleComponent } from './workspace/component/result-panel-toggle/result-panel-toggle.component';

import { DashboardComponent } from './dashboard/component/dashboard.component';
import { TopBarComponent } from './dashboard/component/top-bar/top-bar.component';
import { UserAccountIconComponent } from './dashboard/component/top-bar/user-account-icon/user-account-icon.component';
import { FeatureBarComponent } from './dashboard/component/feature-bar/feature-bar.component';

@NgModule({
  declarations: [
    AppComponent,
    WorkspaceComponent,
    NavigationComponent,
    OperatorPanelComponent,
    PropertyEditorComponent,
    WorkflowEditorComponent,
    ResultPanelComponent,
    OperatorLabelComponent,

    DashboardComponent,
    TopBarComponent,
    UserAccountIconComponent,
    FeatureBarComponent,

    NgbModalComponent,
    OperatorLabelComponent,
    ProductTourComponent,
    ResultPanelToggleComponent
  ],
  imports: [
    BrowserModule,
    AppRoutingModule,
    HttpClientModule,

    CustomNgMaterialModule,
    BrowserAnimationsModule,
    NgbModule.forRoot(),
    RouterModule.forRoot([]),
    TourNgBootstrapModule.forRoot(),

    MaterialDesignFrameworkModule

  ],
  entryComponents: [
    NgbModalComponent
  ],
  providers: [ HttpClientModule ],
  bootstrap: [AppComponent],
  // dynamically created component must be placed in the entryComponents attribute
})
export class AppModule { }
