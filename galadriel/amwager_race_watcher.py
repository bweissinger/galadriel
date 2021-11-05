from datetime import datetime
import time
import random
import operator

from threading import Thread
from zoneinfo import ZoneInfo
from bs4 import BeautifulSoup
from selenium import webdriver
from pymonad.tools import curry
from pymonad.either import Right

from galadriel import database, amwager_scraper


class RaceWatcher(Thread):
    def _go_to_race(self) -> None:
        self.driver.get("https://pro.amwager.com")

        # Cookies must be added after navigating to domain
        for cookie in self.cookies:
            self.driver.add_cookie(cookie)

        self.driver.get(
            "https://pro.amwager.com/#wager/"
            + self.race.meet.track.amwager
            + "/"
            + str(self.race.race_num)
        )
        time.sleep(random.randint(3, 10))
        self.driver.refresh()

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
        [table_list.bind(database.add_and_commit) for table_list in tables]

    @curry(3)
    def _update_runners(self, soup, race_status):
        self.runners = amwager_scraper.update_scratched_status(
            soup, self.runners
        ).either(lambda x: Right(self.runners), lambda x: Right(x))
        if race_status["results_posted"]:
            self.runners = self.runners.bind(amwager_scraper.scrape_results(soup))
        self.runners = self.runners.bind(database.update_models).either(
            lambda x: self.terminate_now(), lambda x: x
        )

    def terminate_now(self):
        self.terminate = True

    def run(self):
        def _check_race_status(race_status):
            if race_status["results_posted"]:
                self.terminate_now()
            return Right(race_status)

        database.setup_db(self.path)
        self.race = database.Race.query.get(self.race_id)
        if self.race == None:
            raise ValueError("Could not find race with id: %s" % self.race_id)
        if self.race.runners == []:
            raise ValueError("Race with id '%s' contains no runners." % self.race_id)
        self.runners = sorted(self.race.runners, key=operator.attrgetter("tab"))
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
        self._go_to_race()
        while not self.terminate:
            datetime_retrieved = datetime.now(ZoneInfo("UTC"))
            soup = BeautifulSoup(self.driver.page_source, "lxml")
            seconds_since_update = amwager_scraper.scrape_seconds_since_update(
                soup
            ).either(lambda x: None, lambda x: x)
            try:
                if seconds_since_update > 60:
                    self.driver.refresh()
                    time.sleep(5)
            except TypeError:
                self.driver.refresh()
                time.sleep(5)
                continue
            race_status = amwager_scraper.get_race_status(soup, datetime_retrieved)
            race_status.bind(_check_race_status).bind(
                self._scrape_data(soup, datetime_retrieved)
            )
            race_status.bind(self._update_runners(soup))
            race_status.bind(_check_race_status)
            time.sleep(10)
        database.close_db()
        self.driver.quit()

    def __init__(self, race_id, database_path, cookies):
        Thread.__init__(self)
        self.terminate = False

        profile = webdriver.FirefoxProfile()
        profile.set_preference("dom.webdriver.enabled", False)
        profile.set_preference("useAutomationExtension", False)
        profile.update_preferences()
        desired = webdriver.DesiredCapabilities.FIREFOX
        self.driver = webdriver.Firefox(
            firefox_profile=profile, desired_capabilities=desired
        )
        self.path = database_path
        self.cookies = cookies
        self.race_id = race_id
