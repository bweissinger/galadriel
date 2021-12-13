from datetime import datetime
import time
import operator
import logging
import os

from threading import Thread
from zoneinfo import ZoneInfo
from bs4 import BeautifulSoup
from selenium import webdriver
from pymonad.tools import curry
from pymonad.either import Right
from selenium.common.exceptions import StaleElementReferenceException, TimeoutException
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait

from galadriel import database, amwager_scraper

logger = logging.getLogger("RACE_WATCHER_LOGGER")


class RaceWatcher(Thread):
    def _prep_domain(self) -> None:
        self.driver.get("https://pro.amwager.com")

        # Cookies must be added after navigating to domain
        for cookie in self.cookies:
            self.driver.add_cookie(cookie)

    def _go_to_race(self) -> None:
        for x in range(0, 5):
            try:
                url = (
                    "https://pro.amwager.com/#wager/"
                    + self.race.meet.track.amwager
                    + "/"
                    + str(self.race.race_num)
                )
                if self.driver.current_url != url:
                    self.driver.get(url)
                else:
                    self.driver.refresh()
                WebDriverWait(self.driver, 30).until(
                    lambda x: "track-num-fucus"
                    in x.find_element(
                        By.ID,
                        "race-%s" % self.race.race_num,
                    ).get_attribute("class")
                )

                def _track_focused(driver):
                    soup = BeautifulSoup(driver.page_source, "lxml")
                    elements = []
                    try:
                        elements.append(
                            soup.find(
                                "button",
                                class_="am-intro-race-mobile btn dropdowntrack dropdown-toggle dropdown-small btn-track-xs",
                            ).text
                        )
                    except AttributeError:
                        pass
                    try:
                        elements.append(soup.find("span", {"class": "eventName"}).text)
                    except AttributeError:
                        pass
                    return self.race.meet.track.amwager_list_display in elements

                WebDriverWait(self.driver, 30).until(_track_focused)
                break
            except (StaleElementReferenceException, TimeoutException):
                if x == 4:
                    raise

    @curry(4)
    def _scrape_data(self, soup, datetime_retrieved, race_status):
        # get all the tables and add to database
        tables = [
            amwager_scraper.scrape_odds(race_status, soup, self.runners).bind(
                database.pandas_df_to_models(database.AmwagerIndividualOdds)
            ),
            amwager_scraper.scrape_individual_pools(
                race_status, soup, self.runners
            ).bind(database.pandas_df_to_models(database.IndividualPool)),
            amwager_scraper.scrape_exacta_odds(soup, self.runners, race_status).bind(
                database.pandas_df_to_models(database.ExactaOdds)
            ),
            amwager_scraper.scrape_quinella_odds(soup, self.runners, race_status).bind(
                database.pandas_df_to_models(database.QuinellaOdds)
            ),
            amwager_scraper.scrape_exotic_totals(soup, self.race.id, race_status).bind(
                database.pandas_df_to_models(database.ExoticTotals)
            ),
            amwager_scraper.scrape_willpays(
                soup, self.runners, datetime_retrieved
            ).bind(database.pandas_df_to_models(database.WillpayPerDollar)),
            amwager_scraper.scrape_race_commissions(
                soup, self.race.id, datetime_retrieved
            ).bind(database.pandas_df_to_models(database.RaceCommission)),
        ]
        if self.race_2_runners:
            tables.append(
                amwager_scraper.scrape_double_odds(
                    soup, self.runners, self.race_2_runners, race_status
                ).bind(database.pandas_df_to_models(database.DoubleOdds))
            )
        if race_status["results_posted"]:
            tables.append(
                amwager_scraper.scrape_payouts(soup, self.race.id, datetime_retrieved)
            )
        for table_list in tables:
            # Maybe flatten then add?
            table_list.bind(database.add_and_commit(self.session))

    @curry(3)
    def _update_runners(self, soup, race_status):
        self.runners = amwager_scraper.update_scratched_status(
            soup, self.runners
        ).either(lambda x: Right(self.runners), lambda x: Right(x))
        if race_status["results_posted"]:
            self.runners = self.runners.bind(amwager_scraper.scrape_results(soup))
        self.runners = self.runners.bind(database.update_models(self.session)).either(
            lambda x: self.terminate_now(), lambda x: x
        )

    def terminate_now(self):
        self.terminate = True

    def _watch_race(self):
        def _check_race_status(race_status):
            if race_status["results_posted"]:
                self.terminate_now()
            return Right(race_status)

        self.race = self.session.query(database.Race).get(self.race_id)
        if self.race == None:
            raise ValueError("Could not find race with id: %s" % self.race_id)
        self.runners = sorted(self.race.runners, key=operator.attrgetter("tab"))

        self._prep_domain()
        self._go_to_race()

        if not self.runners:
            soup = BeautifulSoup(self.driver.page_source, "lxml")
            self.runners = (
                amwager_scraper.scrape_runners(soup, self.race.id)
                .bind(database.pandas_df_to_models)
                .bind(database.add_and_commit(self.session))
                .either(lambda x: None, lambda x: x)
            )
            if not self.runners:
                raise ValueError(
                    "Race with id '%s' contains no runners." % self.race_id
                )
        self.race_2 = next(
            (
                race
                for race in self.race.meet.races
                if race.race_num == self.race.race_num + 1
            ),
            None,
        )
        if self.race_2:
            self.race_2_runners = sorted(
                self.race_2.runners, key=operator.attrgetter("tab")
            )
        else:
            self.race_2_runners = None

        # Scrape twinspires

        while not self.terminate:
            datetime_retrieved = datetime.now(ZoneInfo("UTC"))
            soup = BeautifulSoup(self.driver.page_source, "lxml")
            seconds_since_update = amwager_scraper.scrape_seconds_since_update(
                soup
            ).either(lambda x: -1, lambda x: x)
            if seconds_since_update > 60:
                self.driver.refresh()
                self._go_to_race()
                continue
            race_status = amwager_scraper.get_race_status(soup, datetime_retrieved)
            race_status.bind(_check_race_status).bind(
                self._scrape_data(soup, datetime_retrieved)
            )
            race_status.bind(self._update_runners(soup))
            race_status.bind(_check_race_status)
            time.sleep(10)

    def _destroy(self):
        self.session.close()
        self.driver.quit()

    def run(self):
        try:
            self.session = database.Session()
            self._watch_race()
        except Exception:
            logger.exception(
                "Exception while watching race with ID '%s'" % self.race_id
            )
            self.terminate_now()
        self._destroy()

    def __init__(self, race_id: int, cookies: dict, log_path: str = ""):
        Thread.__init__(self)
        self.terminate = False
        self.cookies = cookies
        self.race_id = race_id
        self.race = None

        fh = logging.FileHandler(os.path.join(log_path, "race_watcher.log"))
        formatter = logging.Formatter(
            "%(asctime)s | %(name)s | %(levelname)s | %(message)s"
        )
        fh.setFormatter(formatter)
        logger.addHandler(fh)

        profile = webdriver.FirefoxProfile()
        profile.set_preference("dom.webdriver.enabled", False)
        profile.set_preference("useAutomationExtension", False)
        profile.update_preferences()
        desired = webdriver.DesiredCapabilities.FIREFOX
        self.driver = webdriver.Firefox(
            firefox_profile=profile, desired_capabilities=desired
        )
