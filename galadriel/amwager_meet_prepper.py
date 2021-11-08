import random
import time

from threading import Thread
from bs4 import BeautifulSoup
from selenium import webdriver
from datetime import datetime
from zoneinfo import ZoneInfo
from pymonad.either import Right

from galadriel import database, amwager_scraper


class MeetPrepper(Thread):
    def _prepare_domain(self):
        self.driver.get("https://pro.amwager.com")

        # Cookies must be added after navigating to domain
        for cookie in self.cookies:
            self.driver.add_cookie(cookie)

        self.driver.get("https://pro.amwager.com/#wager/" + self.track.amwager)

    def _go_to_race(self, race_num) -> None:
        self.driver.get(
            "https://pro.amwager.com/#wager/" + self.track.amwager + "/" + str(race_num)
        )
        time.sleep(random.randint(3, 10))
        self.driver.refresh()

    def run(self):
        database.setup_db(self.db_path)
        self.track = database.Track.query.get(self.track_id)
        if not self.track:
            raise ValueError("Could not find track with id '%s'" % self.track)
        self._prepare_domain()

        self.meet = database.add_and_commit(
            database.Meet(
                local_date=datetime.now(ZoneInfo(self.track.timezone)).date(),
                track_id=self.track.id,
                datetime_retrieved=datetime.now(ZoneInfo("UTC")),
            )
        ).either(lambda x: self.terminate_now(), lambda x: x[0])
        print(self.meet)

        for try_count in range(0, 5):
            time.sleep(random.randint(10, 20))
            soup = BeautifulSoup(self.driver.page_source, "lxml")
            self.num_races = amwager_scraper.get_num_races(soup).either(
                lambda x: None, lambda x: x
            )
            if self.num_races:
                break
            elif try_count == 4:
                raise ValueError("Could not find num races in meet.")

        races = []
        for race_num in range(1, self.num_races + 1):
            if self.terminate:
                break
            self._go_to_race(race_num)
            time.sleep(random.randint(5, 10))
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
