import argparse
import time
import random
import logging
import os

from bs4 import BeautifulSoup
from selenium import webdriver
from zoneinfo import ZoneInfo
from datetime import datetime, timedelta
from sqlalchemy import and_

from galadriel import (
    amwager_meet_prepper,
    amwager_race_watcher,
    database,
    amwager_scraper,
)

global session
logger = logging.getLogger("MISSING_TRACKS_LOGGER")


def _get_todays_meets_not_ignored() -> list[database.Meet]:
    def _meet_is_today(meet: database.Meet) -> bool:
        timezone = ZoneInfo(meet.track.timezone)
        today = datetime.now(timezone).date()
        if meet.local_date == today:
            return True
        return False

    today = datetime.now(ZoneInfo("UTC")).date()
    meets = (
        session.query(database.Meet)
        .filter(
            and_(
                database.Meet.local_date >= today - timedelta(days=1),
                database.Meet.local_date <= today + timedelta(days=1),
            )
        )
        .all()
    )
    return [meet for meet in meets if _meet_is_today(meet)]


def _get_tracks_to_scrape(amwager_meets: list[dict[str, str]]) -> list[database.Meet]:
    all_tracks = session.query(database.Track).all()
    meet_already_added = [meet.track for meet in _get_todays_meets_not_ignored()]

    def _get_track_in_database(meet):
        for track in all_tracks:
            if meet["id"] == track.amwager:
                all_tracks.remove(track)
                return track

    to_watch = []
    for meet in amwager_meets:
        track = _get_track_in_database(meet)
        if not track:
            logger.warning(
                "Track '%s' not in database. Full amwager listing: %s"
                % (meet["id"], meet)
            )
        elif not track.ignore and track not in meet_already_added:
            to_watch.append(track)

    return to_watch


def _prep_meets(tracks_to_prep: list[database.Track]) -> None:
    currently_prepping = []
    while tracks_to_prep or currently_prepping:
        if tracks_to_prep and len(currently_prepping) < 5:
            prepper_thread = amwager_meet_prepper.MeetPrepper(
                tracks_to_prep.pop().id, driver.get_cookies(), cmd_args.log_dir
            )
            currently_prepping.append(prepper_thread)
            prepper_thread.start()
        for prepper in currently_prepping:
            if not prepper.is_alive():
                currently_prepping.remove(prepper)
        time.sleep(random.randint(3, 7))


def _get_todays_races_without_results() -> list[database.Race]:
    races = []
    for meet in _get_todays_meets_not_ignored():
        for race in meet.races:
            if not database.has_results(race):
                races.append(race)
    return races


def _watch_races(races_to_watch: list[database.Race]) -> None:
    watching = []
    while watching or races_to_watch:
        dt_now = datetime.now(ZoneInfo("UTC"))
        for race in races_to_watch:
            est_post = race.estimated_post.replace(tzinfo=ZoneInfo("UTC"))
            if est_post - dt_now <= timedelta(minutes=15):
                if not (est_post <= dt_now and not race.runners):
                    # No point in getting results for races that have no runners
                    # present and are already posted
                    watcher_thread = amwager_race_watcher.RaceWatcher(
                        race.id, driver.get_cookies(), cmd_args.log_dir
                    )
                    watching.append(watcher_thread)
                    watcher_thread.start()
                races_to_watch.remove(race)
        for watcher in watching:
            if not watcher.is_alive():
                watching.remove(watcher)
        time.sleep(random.randint(3, 7))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("db_path", metavar="db_path", type=str)
    parser.add_argument("--log_dir", type=str, default="")
    parser.add_argument("--missing_only", default=False, action="store_true")
    cmd_args = parser.parse_args()

    fh = logging.FileHandler(os.path.join(cmd_args.log_dir, "missing_tracks.log"))
    formatter = logging.Formatter(
        "%(asctime)s | %(name)s | %(levelname)s | %(message)s"
    )
    fh.setFormatter(formatter)
    logger.addHandler(fh)

    database.setup_db(cmd_args.db_path, cmd_args.log_dir)

    session = database.Session()

    profile = webdriver.FirefoxProfile()
    profile.set_preference("dom.webdriver.enabled", False)
    profile.set_preference("useAutomationExtension", False)
    profile.update_preferences()
    desired = webdriver.DesiredCapabilities.FIREFOX
    driver = webdriver.Firefox(firefox_profile=profile, desired_capabilities=desired)
    driver.get("https://pro.amwager.com/#wager")

    input("Press Enter to continue after login...")

    soup = BeautifulSoup(driver.page_source, "lxml")

    tracks_to_scrape = amwager_scraper.get_track_list(soup).bind(_get_tracks_to_scrape)

    # All missing tracks will have already been logged when getting tracks_to_scrape
    if not cmd_args.missing_only:
        _prep_meets(tracks_to_scrape)
        _watch_races(_get_todays_races_without_results())

    database.Session.remove()
    driver.quit()
