import { Injectable } from '@angular/core';
import { Response, Http } from '@angular/http';

import { Observable } from 'rxjs/Observable';
import { UserDictionary } from '../../type/user-dictionary';

import { MOCK_USER_DICTIONARY_LIST } from './mock-user-dictionary.data';

@Injectable()
export class StubUserDictionaryService {

  constructor() { }

}
