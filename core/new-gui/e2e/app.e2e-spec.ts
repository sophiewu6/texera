import { AppPage } from './app.po';
import { browser } from 'protractor';

describe('new-gui App', () => {
  let page: AppPage;

  beforeEach(() => {
    page = new AppPage();
  });

  it('should display welcome message', () => {
    page.navigateTo();
    expect(page.getTexeraTitleText()).toEqual('Texera');
  });

  fit('should display tutorial correctly', () => {
    page.navigateTo();
    page.getTutorialButton().click();
    // expect(page.getTutorialNextButton().getText()).toEqual('Next »');
    // expect(page.getTutorialEndButton().getText()).toEqual('End');
    browser.driver.sleep(10000);
    browser.waitForAngular();
    page.getTutorialNextButton().click();

    browser.driver.sleep(10000);
    browser.waitForAngular();
    // page.getTutorialNextButton().click();
    // page.getTutorialNextButton().click();
    // browser.driver.sleep(3000);
    // browser.waitForAngular();
    // expect(page.getTutorialPrevButton().getText()).toEqual('« Prev');
    // page.getTutorialPrevButton().click();
  });
});
