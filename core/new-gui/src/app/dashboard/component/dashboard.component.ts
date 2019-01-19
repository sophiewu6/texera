import { Component, OnInit } from '@angular/core';

<<<<<<< HEAD
import { UserDictionaryService } from '../service/user-dictionary/user-dictionary.service';
import { StubUserDictionaryService } from '../service/user-dictionary/stub-user-dictionary.service';
import { SavedProjectService } from '../service/saved-project/saved-project.service';
import { StubSavedProjectService } from '../service/saved-project/stub-saved-project.service';


=======
>>>>>>> 6c4d02b9401591e6cfce681569a8504b4b35bcd2
@Component({
  selector: 'texera-dashboard',
  templateUrl: './dashboard.component.html',
  styleUrls: ['./dashboard.component.scss'],
  providers: [
      // UserDictionaryService,
      { provide: UserDictionaryService, useClass: StubUserDictionaryService },
      { provide: SavedProjectService, useClass: StubSavedProjectService },
      // SavedProjectService,
      StubSavedProjectService
    ]
})
export class DashboardComponent implements OnInit {

  constructor() { }

  ngOnInit() {
  }

}
