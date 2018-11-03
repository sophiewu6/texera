import { browser, by, element } from 'protractor';

export class AppPage {
  navigateTo() {
    return browser.get('/');
  }

  getTexeraTitleText() {
    return element(by.css('.navbar-brand')).getText();
  }
}
