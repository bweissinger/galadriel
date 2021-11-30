import argparse
import time
from bs4 import BeautifulSoup

from selenium import webdriver
from zoneinfo import ZoneInfo
from datetime import datetime, timedelta
from sqlalchemy import and_
from pymonad.tools import curry

from galadriel import (
    amwager_meet_prepper,
    amwager_race_watcher,
    database,
    amwager_scraper,
)


def _get_todays_meets_in_database() -> list[database.Meet]:
    def _meet_is_today(meet: database.Meet) -> bool:
        timezone = ZoneInfo(meet.track.timezone)
        today = datetime.now(timezone).date()
        if meet.local_date == today:
            return True
        return False

    today = datetime.now(ZoneInfo("UTC")).date()
    meets = database.Meet.query.filter(
        and_(
            database.Meet.local_date >= today - timedelta(days=1),
            database.Meet.local_date <= today + timedelta(days=1),
        )
    ).all()
    return [meet for meet in meets if _meet_is_today(meet)]


def _get_tracks_to_scrape(amwager_meets: list[dict[str, str]]) -> list[database.Meet]:
    listed_track_names = [meet["id"] for meet in amwager_meets]
    in_database = database.Track.query.filter(database.Track.ignore.is_(False))
    return [track for track in in_database if track.amwager in listed_track_names]


def _prep_meets(tracks_to_prep: list[database.Meet]) -> None:
    currently_prepping = []
    while tracks_to_prep or currently_prepping:
        if len(currently_prepping) < 5:
            currently_prepping.append(
                amwager_meet_prepper.MeetPrepper(
                    tracks_to_prep.pop().id, driver.get_cookies(), cmd_args.db_path
                ).run()
            )
        for prepper in currently_prepping:
            if not prepper.is_alive():
                currently_prepping.remove(prepper)
        time.sleep(10)


def _get_todays_races_without_results() -> list[database.Race]:
    races = []
    for meet in _get_todays_meets_in_database():
        for race in meet.races:
            if not database.has_results(race):
                races.append(race)
    return races


def _watch_races(races_to_watch: list[database.Race]) -> None:
    watching = []
    while watching or races_to_watch:
        dt_now = datetime.now(ZoneInfo("UTC"))
        for race in races_to_watch:
            if race.estimated_post - dt_now <= timedelta(minutes=15):
                watching.append(
                    amwager_race_watcher.RaceWatcher(
                        race.id, cmd_args.db_path, driver.get_cookies()
                    ).run()
                )
            races_to_watch.remove(race)
        for watcher in watching:
            if not watcher.is_alive():
                watching.remove(watcher)
        time.sleep(30)


def _setup_db(path):
    database.setup_db(path)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("db_path", metavar="db_path", type=str)
    cmd_args = parser.parse_args()

    _setup_db(cmd_args.db_path)

    profile = webdriver.FirefoxProfile()
    profile.set_preference("dom.webdriver.enabled", False)
    profile.set_preference("useAutomationExtension", False)
    profile.update_preferences()
    desired = webdriver.DesiredCapabilities.FIREFOX
    driver = webdriver.Firefox(firefox_profile=profile, desired_capabilities=desired)
    driver.get("https://pro.amwager.com/#wager")

    input("Press Enter to continue after login...")

    soup = BeautifulSoup(driver.page_source, "lxml")

    amwager_scraper.get_track_list(soup).bind(_get_tracks_to_scrape).bind(_prep_meets)

    _watch_races(_get_todays_races_without_results())
