import argparse
import time
import logging
import os
import psutil

from bs4 import BeautifulSoup
from selenium import webdriver
from datetime import datetime, timedelta
from sqlalchemy import and_
from sqlalchemy.orm import scoped_session
from pymonad.tools import curry
from typing import List, Dict

try:
    from zoneinfo import ZoneInfo
except ImportError:
    from backports.zoneinfo import ZoneInfo

from galadriel import (
    amwager_meet_prepper,
    amwager_race_watcher,
    database,
    amwager_scraper,
)

logger_missing_tracks = logging.getLogger("MISSING_TRACKS_LOGGER")
logger_main = logging.getLogger("MAIN_LOGGER")


def _get_todays_meets_not_ignored(session: scoped_session) -> List[database.Meet]:
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


@curry(2)
def _get_tracks_to_scrape(
    session: scoped_session, amwager_meets: List[Dict[str, str]]
) -> List[database.Meet]:
    all_tracks = session.query(database.Track).all()
    meet_already_added = [meet.track for meet in _get_todays_meets_not_ignored(session)]

    def _get_track_in_database(meet):
        for track in all_tracks:
            if meet["id"] == track.amwager:
                all_tracks.remove(track)
                return track

    to_watch = []
    for meet in amwager_meets:
        track = _get_track_in_database(meet)
        if not track:
            logger_missing_tracks.warning(
                "Track '%s' not in database. Full amwager listing: %s"
                % (meet["id"], meet)
            )
        elif not track.ignore and track not in meet_already_added:
            to_watch.append(track)

    return to_watch


def _prep_meets(tracks_to_prep: List[database.Track]) -> None:
    currently_prepping = []
    start_time = time.time()
    while tracks_to_prep or currently_prepping:
        for prepper in currently_prepping:
            if not prepper.is_alive():
                currently_prepping.remove(prepper)
        for track in tracks_to_prep:
            if (
                len(currently_prepping) < cmd_args.max_preppers
                and psutil.virtual_memory().percent < cmd_args.max_memory_percent
            ):
                try:
                    prepper_thread = amwager_meet_prepper.MeetPrepper(
                        track.id, driver.get_cookies(), cmd_args.log_dir
                    )
                    currently_prepping.append(prepper_thread)
                    prepper_thread.start()
                    tracks_to_prep.remove(track)
                except Exception:
                    logger_main.exception("Failed to run meet_prepper.")
                    continue
        current_time = time.time()
        if current_time - start_time > 600:
            driver.refresh()
            start_time = current_time
        time.sleep(5)


def _get_todays_races_without_results(session: scoped_session) -> List[database.Race]:
    races = []
    for meet in _get_todays_meets_not_ignored(session):
        for race in meet.races:
            if not database.has_results(race):
                races.append(race)
    return races


def _watch_races(races_to_watch: List[database.Race]) -> None:
    watching = []
    dt_now = datetime.now(ZoneInfo("UTC"))
    start_time = time.time()
    while watching or races_to_watch:
        for watcher in watching:
            if not watcher.is_alive():
                watching.remove(watcher)
        for race in races_to_watch:
            if (
                len(watching) < cmd_args.max_watchers
                and psutil.virtual_memory().percent < cmd_args.max_memory_percent
            ):
                dt_now = datetime.now(ZoneInfo("UTC"))
                est_post = race.estimated_post.replace(tzinfo=ZoneInfo("UTC"))
                if est_post - dt_now <= timedelta(minutes=5):
                    if not (est_post <= dt_now and not race.runners):
                        try:
                            # No point in getting results for races that have no runners
                            # present and are already posted
                            watcher_thread = amwager_race_watcher.RaceWatcher(
                                race.id, driver.get_cookies(), cmd_args.log_dir
                            )
                            watching.append(watcher_thread)
                            watcher_thread.start()
                        except Exception:
                            logger_main.exception("Failed to run race_watcher.")
                            continue
                    races_to_watch.remove(race)
        current_time = time.time()
        if current_time - start_time > 600:
            driver.refresh()
            start_time = current_time
        time.sleep(15)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("db_path", metavar="db_path", type=str)
    parser.add_argument("--log_dir", type=str, default="")
    parser.add_argument("--missing_only", default=False, action="store_true")
    parser.add_argument("--max_preppers", type=int, default=4)
    parser.add_argument("--max_watchers", type=int, default=12)
    parser.add_argument("--max_memory_percent", type=int, default=80)
    cmd_args = parser.parse_args()
    cmd_args.db_path = "sqlite:///%s" % cmd_args.db_path
    cmd_args.max_memory_percent = (
        100 if cmd_args.max_memory_percent > 100 else cmd_args.max_memory_percent
    )

    fh = logging.FileHandler(os.path.join(cmd_args.log_dir, "missing_tracks.log"))
    formatter = logging.Formatter(
        "%(asctime)s | %(name)s | %(levelname)s | %(message)s"
    )
    fh.setFormatter(formatter)
    logger_missing_tracks.addHandler(fh)

    fh = logging.FileHandler(os.path.join(cmd_args.log_dir, "main_logger.log"))
    formatter = logging.Formatter(
        "%(asctime)s | %(name)s | %(levelname)s | %(message)s"
    )
    fh.setFormatter(formatter)
    logger_main.addHandler(fh)

    database.setup_db(cmd_args.db_path, cmd_args.log_dir)

    options = webdriver.ChromeOptions()
    options.add_argument("--no-sandbox")
    options.add_argument("--start-maximized")
    options.add_argument("--single-process")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--incognito")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_experimental_option("useAutomationExtension", False)
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_argument("disable-infobars")

    driver = webdriver.Chrome(options=options)
    driver.get("https://pro.amwager.com/#wager")

    input("Press Enter to continue after login...")

    soup = BeautifulSoup(driver.page_source, "lxml")
    session = database.Session()
    tracks_to_scrape = amwager_scraper.get_track_list(soup).bind(
        _get_tracks_to_scrape(session)
    )

    # All missing tracks will have already been logged when getting tracks_to_scrape
    if not cmd_args.missing_only:
        _prep_meets(tracks_to_scrape)
        _watch_races(_get_todays_races_without_results(session))

    session.close()
    driver.quit()
