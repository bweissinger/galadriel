import unittest
import yaml
import pandas

from bs4 import BeautifulSoup
from os import path
from datetime import datetime
from zoneinfo import ZoneInfo
from tzlocal import get_localzone
from pymonad.tools import curry

from galadriel import amwager_scraper as scraper
from galadriel import database
from galadriel import resources as galadriel_res

RES_PATH = "./tests/resources"
with open(path.join(RES_PATH, "test_amwager_scraper.yml"), "r") as yaml_file:
    YAML_VARS = yaml.safe_load(yaml_file)


def _create_soups() -> list[BeautifulSoup]:
    soups = {}
    for name in YAML_VARS["SoupList"]:
        file_path = path.join(RES_PATH, ("amw_%s.html" % name))
        with open(file_path, "r") as html:
            soups[name] = BeautifulSoup(html.read(), "lxml")
    return soups


SOUPS = _create_soups()


def add_prep_models():
    dt = datetime.now(ZoneInfo("UTC"))
    database.add_and_commit(database.Country(name="US"))
    database.add_and_commit(
        database.Track(name="PIM", country_id=1, timezone="America/Chicago")
    )
    datetime.now(ZoneInfo("UTC"))
    database.add_and_commit(
        database.Meet(datetime_retrieved=dt, local_date=dt.date(), track_id=1)
    )
    database.add_and_commit(database.Platform(name="amwager"))
    database.add_and_commit(database.Discipline(name="Greyhound"))
    database.add_and_commit(database.Discipline(name="Harness"))
    database.add_and_commit(database.Discipline(name="Tbred"))


@curry(2)
def create_and_add(model: database.Base, df: pandas.DataFrame):
    return database.pandas_df_to_models(df, model).bind(database.add_and_commit)


class DBTestCase(unittest.TestCase):
    def setUp(self):
        super().setUp()
        database.setup_db("sqlite:///:memory:")

    def tearDown(self):
        database.Base.metadata.drop_all(bind=database.engine)
        super().tearDown()


class TestMtpListed(DBTestCase):
    def test_scraping(self):
        add_prep_models()
        soup = SOUPS["mtp_listed"]
        dt_retrieved = datetime.now(ZoneInfo("UTC"))
        race_status = scraper.get_race_status(soup, dt_retrieved).bind(lambda x: x)
        race = (
            scraper.scrape_race(soup, dt_retrieved, 1)
            .bind(create_and_add(database.Race))
            .bind(lambda x: x)
        )[0]
        runners = (
            scraper.scrape_runners(soup, race.id)
            .bind(create_and_add(database.Runner))
            .bind(lambda x: x)
        )
        odds = scraper.scrape_odds(race_status, soup, runners).bind(
            create_and_add(database.AmwagerIndividualOdds)
        )
        individual_pools = scraper.scrape_individual_pools(
            race_status, soup, runners, 1
        ).bind(create_and_add(database.IndividualPool))
        exotic_totals = scraper.scrape_exotic_totals(
            soup, race.id, 1, race_status
        ).bind(create_and_add(database.ExoticTotals))
        self.assertTrue(odds.is_right() is True)
        self.assertTrue(individual_pools.is_right() is True)
        self.assertTrue(exotic_totals.is_right() is True)


class TestPostTimeListed(DBTestCase):
    def test_scraping(self):
        add_prep_models()
        soup = SOUPS["post_time_listed"]
        dt_retrieved = datetime.now(ZoneInfo("UTC"))
        race_status = scraper.get_race_status(soup, dt_retrieved).bind(lambda x: x)
        race = (
            scraper.scrape_race(soup, dt_retrieved, 1)
            .bind(create_and_add(database.Race))
            .bind(lambda x: x)
        )[0]
        runners = (
            scraper.scrape_runners(soup, race.id)
            .bind(create_and_add(database.Runner))
            .bind(lambda x: x)
        )
        odds = scraper.scrape_odds(race_status, soup, runners).bind(
            create_and_add(database.AmwagerIndividualOdds)
        )
        individual_pools = scraper.scrape_individual_pools(
            race_status, soup, runners, 1
        ).bind(create_and_add(database.IndividualPool))
        exotic_totals = scraper.scrape_exotic_totals(
            soup, race.id, 1, race_status
        ).bind(create_and_add(database.ExoticTotals))
        self.assertTrue(odds.is_right() is True)
        self.assertTrue(individual_pools.is_right() is True)
        self.assertTrue(exotic_totals.is_right() is True)


class TestWageringClosed(DBTestCase):
    def test_scraping(self):
        add_prep_models()
        soup = SOUPS["wagering_closed"]
        dt_retrieved = datetime.now(ZoneInfo("UTC"))
        race_status = scraper.get_race_status(soup, dt_retrieved).bind(lambda x: x)
        race = (
            scraper.scrape_race(soup, dt_retrieved, 1)
            .bind(create_and_add(database.Race))
            .bind(lambda x: x)
        )[0]
        runners = (
            scraper.scrape_runners(soup, race.id)
            .bind(create_and_add(database.Runner))
            .bind(lambda x: x)
        )
        odds = scraper.scrape_odds(race_status, soup, runners).bind(
            create_and_add(database.AmwagerIndividualOdds)
        )
        individual_pools = scraper.scrape_individual_pools(
            race_status, soup, runners, 1
        ).bind(create_and_add(database.IndividualPool))
        exotic_totals = scraper.scrape_exotic_totals(
            soup, race.id, 1, race_status
        ).bind(create_and_add(database.ExoticTotals))
        self.assertTrue(odds.is_right() is True)
        self.assertTrue(individual_pools.is_right() is True)
        self.assertTrue(exotic_totals.is_right() is True)


class TestResultsPosted(DBTestCase):
    def test_scraping(self):
        add_prep_models()
        soup = SOUPS["results_posted"]
        dt_retrieved = datetime.now(ZoneInfo("UTC"))
        race_status = scraper.get_race_status(soup, dt_retrieved).bind(lambda x: x)
        race = (
            scraper.scrape_race(soup, dt_retrieved, 1)
            .bind(create_and_add(database.Race))
            .bind(lambda x: x)
        )[0]
        runner_models = [
            database.Runner(name="runner %s" % x, morning_line="1/9", tab=x, race_id=1)
            for x in range(1, 15)
        ]
        runners = database.add_and_commit(runner_models).bind(lambda x: x)
        odds = scraper.scrape_odds(race_status, soup, runners).bind(
            create_and_add(database.AmwagerIndividualOdds)
        )
        individual_pools = scraper.scrape_individual_pools(
            race_status, soup, runners, 1
        ).bind(create_and_add(database.IndividualPool))
        exotic_totals = scraper.scrape_exotic_totals(
            soup, race.id, 1, race_status
        ).bind(create_and_add(database.ExoticTotals))
        self.assertTrue(odds.is_right() is True)
        self.assertTrue(individual_pools.is_right() is True)
        self.assertTrue(exotic_totals.is_right() is True)
