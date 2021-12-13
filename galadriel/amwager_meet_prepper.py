import time
import logging
import os
import functools

from threading import Thread
from bs4 import BeautifulSoup
from selenium import webdriver
from datetime import datetime
from pymonad.tools import curry
from selenium.webdriver.support.ui import WebDriverWait

try:
    from zoneinfo import ZoneInfo
except ImportError:
    from backports.zoneinfo import ZoneInfo

from galadriel import database, amwager_scraper, racing_and_sports_scraper

logger = logging.getLogger("MEET_PREPPER_LOGGER")


def retry_with_timeout(tries: int, timeout_seconds: int):
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            for x in range(0, tries):
                try:
                    return func(*args, **kwargs)
                except Exception:
                    if x == tries - 1:
                        raise
                time.sleep(timeout_seconds)

        return wrapper

    return decorator


class MeetPrepper(Thread):
    @retry_with_timeout(2, 10)
    def _prepare_domain(self):
        self.driver.get("https://pro.amwager.com")

        # Cookies must be added after navigating to domain
        for cookie in self.cookies:
            self.driver.add_cookie(cookie)

    def _track_focused(self, driver):
        soup = BeautifulSoup(driver.page_source, "lxml")
        elements = []
        # There are two possible locations to determine which track the watcher
        #   is focused on. Search both and check if the correct track is in those
        #   results
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

    # Race focused
    @curry(3)
    def _race_focused(self, race_num, driver):
        soup = BeautifulSoup(driver.page_source, "lxml")
        focused = amwager_scraper.get_focused_race_num(soup).either(
            lambda x: None, lambda x: x
        )
        if focused == race_num:
            return True
        return False

    def _go_to_race(self, race_num) -> None:

        url = "https://pro.amwager.com/#wager/%s/%s" % (
            self.track.amwager,
            race_num,
        )

        if self.driver.current_url != url:
            self.driver.get(url)
        elif not self._race_focused(race_num, self.driver) or not self._track_focused(
            self.driver
        ):
            self.driver.refresh()
        else:
            return

        WebDriverWait(self.driver, 10).until(self._race_focused(race_num))
        WebDriverWait(self.driver, 10).until(self._track_focused)

    def _get_track(self):
        self.track = self.session.query(database.Track).get(self.track_id)
        if not self.track:
            raise ValueError("Could not find track with id '%s'" % self.track_id)

    @retry_with_timeout(10, 2)
    def _add_meet(self):
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
            self.session,
            database.Meet(
                local_date=local_post_date,
                track_id=self.track.id,
                datetime_retrieved=datetime.now(ZoneInfo("UTC")),
            ),
        ).either(lambda x: x, lambda x: x[0])
        if type(self.meet) == str:
            raise ValueError("Could not add meet to database: %s" % self.meet)

    @retry_with_timeout(10, 2)
    def _get_num_races(self):
        self._go_to_race(1)
        soup = BeautifulSoup(self.driver.page_source, "lxml")
        self.num_races = amwager_scraper.get_num_races(soup).either(
            lambda x: x, lambda x: x
        )
        if type(self.num_races) == str:
            raise ValueError("Could not find num races in meet: %s" % self.num_races)

    @retry_with_timeout(10, 2)
    def _results_posted(self, race_num):
        self._go_to_race(race_num)
        soup = BeautifulSoup(self.driver.page_source, "lxml")
        results = amwager_scraper._get_results_posted_status(soup).either(
            lambda x: x, lambda x: x
        )
        if type(results) == str:
            raise ValueError(results)
        return results

    def _add_races_and_runners(self):
        @retry_with_timeout(10, 2)
        def _add_race(race_num):
            self._go_to_race(race_num)
            soup = BeautifulSoup(self.driver.page_source, "lxml")
            datetime_retrieved = datetime.now(ZoneInfo("UTC"))
            race = (
                amwager_scraper.scrape_race(soup, datetime_retrieved, self.meet.id)
                .bind(database.pandas_df_to_models(database.Race))
                .bind(database.add_and_commit(self.session))
                .either(lambda x: x, lambda x: x[0])
            )
            if type(race) is str:
                raise ValueError(race)
            return race

        @retry_with_timeout(10, 2)
        def _add_runners(race_num, race_id):
            self._go_to_race(race_num)
            soup = BeautifulSoup(self.driver.page_source, "lxml")
            runners = (
                amwager_scraper.scrape_runners(soup, race_id)
                .bind(database.pandas_df_to_models(database.Runner))
                .bind(database.add_and_commit(self.session))
                .either(lambda x: x, lambda x: x)
            )
            if type(runners) == str:
                raise ValueError(runners)
            return

        for race_num in range(1, self.num_races + 1):
            try:
                if self._results_posted(race_num):
                    continue
                race = _add_race(race_num)
                _add_runners(race_num, race.id)
            except Exception as e:
                logger.error(
                    "Error while adding race and runners to meet: %s" % e, exc_info=True
                )
                continue

    def _add_rns_data(self):
        @retry_with_timeout(3, 30)
        def _scrape_data():
            result = (
                racing_and_sports_scraper.scrape_meet(self.meet)
                .bind(database.pandas_df_to_models(database.RacingAndSportsRunnerStat))
                .bind(database.add_and_commit(self.session))
                .bind(lambda x: x, lambda x: x)
            )
            if type(result) == str:
                raise ValueError(result)
            return

        self.meet = self.session.query(database.Meet).get(self.meet.id)
        if self.meet.races and self.track.racing_and_sports:
            try:
                _scrape_data()
            except Exception as e:
                logger.error("Could not get rns data: %s" % e, exc_info=True)
        else:
            return

    def _all_races_complete(self):
        try:
            if self._results_posted(1):
                return self._results_posted(self.num_races)
        except Exception:
            pass
        return False

    def run(self):
        try:
            self.session = database.Session()
            self._get_track()
            self._prepare_domain()
            self._add_meet()
            self._get_num_races()
            if not self._all_races_complete():
                self._add_races_and_runners()
                self._add_rns_data()
        except Exception:
            logger.exception(
                "Exception during prepping of meet for track_id '%s'" % self.track_id
            )
            try:
                database.delete_models(self.session, self.meet)
            except AttributeError:
                pass
        self.session.close()
        self.driver.quit()

    def __init__(self, track_id: int, cookies: str, log_path: str = "") -> None:
        Thread.__init__(self)
        self.cookies = cookies
        self.track_id = track_id

        fh = logging.FileHandler(os.path.join(log_path, "meet_prepper.log"))
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
