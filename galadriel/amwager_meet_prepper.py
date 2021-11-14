import random
import time

from threading import Thread
from bs4 import BeautifulSoup
from selenium import webdriver
from datetime import datetime
from zoneinfo import ZoneInfo
from pymonad.either import Right
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions

from galadriel import database, amwager_scraper


class MeetPrepper(Thread):
    def _prepare_domain(self):
        self.driver.get("https://pro.amwager.com")

        # Cookies must be added after navigating to domain
        for cookie in self.cookies:
            self.driver.add_cookie(cookie)

        time.sleep(random.randint(2, 5))
        self._go_to_race(1)

    def _go_to_race(self, race_num) -> None:
        self.driver.get(
            "https://pro.amwager.com/#wager/%s/%s" % (self.track.amwager, race_num)
        )
        element = WebDriverWait(self.driver, 120).until(
            expected_conditions.presence_of_element_located(
                (By.ID, "race-%s" % race_num)
            )
        )
        WebDriverWait(self.driver, 120).until(
            lambda x: element.get_attribute("class")
            == "joemarie btn btn-sm track-num track-num-select track-num-fucus"
        )

    def run(self):
        database.setup_db(self.db_path)
        self.track = database.Track.query.get(self.track_id)
        if not self.track:
            raise ValueError("Could not find track with id '%s'" % self.track)
        self._prepare_domain()

        for try_count in range(0, 5):
            if try_count > 0:
                self.driver.refresh()
                time.sleep(random.randint(20, 60))
            soup = BeautifulSoup(self.driver.page_source, "lxml")
            self.num_races = amwager_scraper.get_num_races(soup).either(
                lambda x: None, lambda x: x
            )
            if self.num_races:
                break
            elif try_count == 4:
                raise ValueError("Could not find num races in meet.")

        time.sleep(random.randint(2, 5))
        self._go_to_race(1)
        soup = BeautifulSoup(self.driver.page_source, "lxml")
        race = amwager_scraper.scrape_race(soup, datetime.now(ZoneInfo("UTC")), 0)
        local_post_date = race.bind(
            lambda x: x.estimated_post[0]
            .to_pydatetime()
            .astimezone(ZoneInfo(self.track.timezone))
            .date()
        )

        self.meet = database.add_and_commit(
            database.Meet(
                local_date=local_post_date,
                track_id=self.track.id,
                datetime_retrieved=datetime.now(ZoneInfo("UTC")),
            )
        ).either(lambda x: self.terminate_now(), lambda x: x[0])

        races = []
        for race_num in range(1, self.num_races + 1):
            time.sleep(random.randint(2, 5))
            if self.terminate:
                break
            self._go_to_race(race_num)
            soup = BeautifulSoup(self.driver.page_source, "lxml")
            datetime_retrieved = datetime.now(ZoneInfo("UTC"))
            race = (
                amwager_scraper.scrape_race(soup, datetime_retrieved, self.meet.id)
                .bind(database.pandas_df_to_models(database.Race))
                .bind(database.add_and_commit)
                .bind(lambda x: Right(x[0]))
            )

            race.bind(lambda x: amwager_scraper.scrape_runners(soup, x.id)).bind(
                database.pandas_df_to_models(database.Runner)
            ).bind(database.add_and_commit).either(
                lambda x: self.terminate_now(), Right
            )
            race.bind(races.append)
        # if self.track.racingandsports:
        #   racing_and_sports_scraper.scrape_meet(meet_id)
        self._check_terminated()
        database.close_db()
        self.driver.quit()

    def _check_terminated(self):
        if self.terminate:
            try:
                database.delete_models(self.meet)
            except AttributeError:
                pass

    def terminate_now(self):
        self.terminate = True

    def __init__(self, track_id, cookies, db_path) -> None:
        Thread.__init__(self)
        self.terminate = False
        self.db_path = db_path
        self.cookies = cookies
        self.track_id = track_id

        profile = webdriver.FirefoxProfile()
        profile.set_preference("dom.webdriver.enabled", False)
        profile.set_preference("useAutomationExtension", False)
        profile.update_preferences()
        desired = webdriver.DesiredCapabilities.FIREFOX
        self.driver = webdriver.Firefox(
            firefox_profile=profile, desired_capabilities=desired
        )
