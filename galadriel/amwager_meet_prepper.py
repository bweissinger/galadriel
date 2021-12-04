import random
import time
import logging

from threading import Thread
from bs4 import BeautifulSoup
from selenium import webdriver
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from pymonad.either import Right
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.common.exceptions import StaleElementReferenceException, TimeoutException

from galadriel import database, amwager_scraper, racing_and_sports_scraper

logger = logging.getLogger(__name__)


class MeetPrepper(Thread):
    def _prepare_domain(self):
        self.driver.get("https://pro.amwager.com")

        # Cookies must be added after navigating to domain
        for cookie in self.cookies:
            self.driver.add_cookie(cookie)

        self._go_to_race(1)

    def _go_to_race(self, race_num) -> None:
        for x in range(0, 5):
            try:
                url = "https://pro.amwager.com/#wager/%s/%s" % (
                    self.track.amwager,
                    race_num,
                )
                if self.driver.current_url != url:
                    self.driver.get(url)
                else:
                    self.driver.refresh()

                WebDriverWait(self.driver, 30).until(
                    lambda x: "track-num-fucus"
                    in x.find_element(
                        By.ID,
                        "race-%s" % race_num,
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
                    return self.track.amwager_list_display in elements

                WebDriverWait(self.driver, 30).until(_track_focused)
                break
            except (StaleElementReferenceException, TimeoutException):
                if x == 4:
                    self.terminate_now()

    def _prep_meet(self):
        database.setup_db(self.db_path)
        self.track = database.Track.query.get(self.track_id)
        if not self.track:
            raise ValueError("Could not find track with id '%s'" % self.track)
        self._prepare_domain()
        if self.terminate:
            return

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
            self._go_to_race(race_num)
            if self.terminate:
                return
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
            ).bind(database.add_and_commit)
            race.bind(races.append)
        if self.track.racing_and_sports and not self.terminate:
            start_dt = datetime.now(ZoneInfo("UTC"))
            while datetime.now(ZoneInfo("UTC")) <= start_dt + timedelta(minutes=5):
                result = (
                    racing_and_sports_scraper.scrape_meet(self.meet)
                    .bind(
                        database.pandas_df_to_models(database.RacingAndSportsRunnerStat)
                    )
                    .bind(database.add_and_commit)
                )
                if result.is_right():
                    break
                time.sleep(60)

    def _destroy(self):
        if self.terminate:
            try:
                database.delete_models(self.meet)
            except AttributeError:
                pass
        database.close_db()
        self.driver.quit()

    def run(self):
        try:
            self._prep_meet()
        except Exception:
            logger.exception("Exception during prepping of meet")
            self.terminate_now()
        self._destroy()

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
