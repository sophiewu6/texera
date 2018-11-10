import { browser, by, element } from 'protractor';

export class AppPage {
  navigateTo() {
    return browser.get('/');
  }

  getTexeraTitleText() {
    return element(by.css('.navbar-brand')).getText();
  }

  getTutorialButton() {
    return element(by.css('.texera-workspace-tour-run'));
  }

  getTutorialNextButton() {
    return element(by.css('.texera-tour-next-button'));
  }

  getTutorialPrevButton() {
    return element(by.css('.texera-tour-prev-button'));
  }

  getTutorialEndButton() {
    return element(by.css('#EndButton'));
  }
}
