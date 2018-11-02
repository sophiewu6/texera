import { Injectable } from '@angular/core';
import { Response, Http } from '@angular/http';

import { Observable } from 'rxjs/Observable';
import { SavedProject } from '../../type/saved-project';

import { MOCK_SAVED_PROJECT_LIST } from './mock-saved-project.data';

@Injectable()
export class StubSavedProjectService {

  constructor() { }
}
