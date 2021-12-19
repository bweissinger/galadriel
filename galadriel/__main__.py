import argparse
import time
import logging
import os
import psutil
import keyring

from getpass import getpass
from bs4 import BeautifulSoup
from selenium import webdriver
from datetime import datetime, timedelta
from sqlalchemy import and_
from sqlalchemy.orm import scoped_session
from pymonad.tools import curry
from typing import List, Dict
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions
from selenium.webdriver.common.by import By
from selenium.common.exceptions import TimeoutException

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
from galadriel.amwager_watcher import retry_with_timeout

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


@retry_with_timeout(5, 120)
def _login():
    def _has_track_list(driver):
        # Selenium cant seem to find the element even though it is visible and
        #   unique
        soup = BeautifulSoup(driver.page_source, "lxml")
        element = soup.find(
            "input",
            {
                "class": "dropdown-toggle dropdowntrack btn-sm amwest-dropdown-input btn-dropdown-track col-xs-12 form-control favorite"
            },
        )
        return element is not None

    try:
        driver = _create_driver()
        driver.get("https://pro.amwager.com/#wager")

        WebDriverWait(driver, 15).until(
            expected_conditions.element_to_be_clickable((By.ID, "email-input-si"))
        ).send_keys(keyring.get_password("galadriel", "username"))
        WebDriverWait(driver, 15).until(
            expected_conditions.element_to_be_clickable((By.ID, "password-input-si"))
        ).send_keys(keyring.get_password("galadriel", "password"))
        WebDriverWait(driver, 15).until(
            expected_conditions.element_to_be_clickable((By.ID, "signIN"))
        ).click()
        WebDriverWait(driver, 30).until(_has_track_list)
        return driver
    except Exception:
        logger_main.exception("Unable to open amwager.com.")
        driver.quit()
        raise


def _set_login():
    keyring.set_password("galadriel", "username", getpass("Username:"))
    keyring.set_password("galadriel", "password", getpass("Password:"))


def _create_driver():
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

    return webdriver.Chrome(options=options)


def _set_logger_formatters():
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


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("db_path", metavar="db_path", type=str)
    parser.add_argument("--log_dir", type=str, default="")
    parser.add_argument("--missing_only", default=False, action="store_true")
    parser.add_argument("--max_preppers", type=int, default=4)
    parser.add_argument("--max_watchers", type=int, default=12)
    parser.add_argument("--max_memory_percent", type=int, default=80)
    parser.add_argument("--set_login", default=False, action="store_true")
    cmd_args = parser.parse_args()
    cmd_args.db_path = "sqlite:///%s" % cmd_args.db_path
    cmd_args.max_memory_percent = (
        100 if cmd_args.max_memory_percent > 100 else cmd_args.max_memory_percent
    )
    return cmd_args


if __name__ == "__main__":
    cmd_args = _parse_args()
    _set_logger_formatters()
    if cmd_args.set_login:
        _set_login()
    driver = _login()

    soup = BeautifulSoup(driver.page_source, "lxml")
    database.setup_db(cmd_args.db_path, cmd_args.log_dir)
    session = database.Session()
    tracks_to_scrape = amwager_scraper.get_track_list(soup).either(
        logger_main.error, _get_tracks_to_scrape(session)
    )

    # All missing tracks will have already been logged when getting tracks_to_scrape
    if not cmd_args.missing_only:
        _prep_meets(tracks_to_scrape)
        _watch_races(_get_todays_races_without_results(session))

    session.close()
    driver.quit()
