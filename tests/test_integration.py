import unittest
from pymonad.either import Either
import yaml
import pandas

from bs4 import BeautifulSoup
from os import path
from datetime import datetime
from zoneinfo import ZoneInfo
from pymonad.tools import curry
from pymonad.either import Right

from galadriel import amwager_scraper as scraper
from galadriel import database

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


def create_next_race_runners(
    current_race: database.Race, num_runners_to_create: int
) -> list[database.Runner]:
    dt = datetime.now(ZoneInfo("UTC"))
    new_race = database.add_and_commit(
        database.Race(
            race_num=current_race.race_num + 1,
            estimated_post=dt,
            discipline_id=1,
            meet_id=current_race.meet_id,
            datetime_retrieved=dt,
        )
    ).bind(lambda x: x)[0]
    return database.add_and_commit(
        [
            database.Runner(
                name="horse %s" % x, morning_line="1/9", tab=x, race_id=new_race.id
            )
            for x in range(1, num_runners_to_create + 1)
        ]
    ).bind(lambda x: x)


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


class TestAmwagerScraperPages(DBTestCase):
    def setUp(self):
        super().setUp()
        self.runner_scraper = scraper.scrape_runners

    def tearDown(self):
        scraper.scrape_runners = self.runner_scraper
        super().tearDown()

    def scrape_race_status_runners(
        self: unittest.TestCase, soup: BeautifulSoup, dt_retrieved: datetime
    ) -> dict[str, object]:
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
        return {"race_status": race_status, "race": race, "runners": runners}

    def scrape_dependent_tables(
        self: unittest.TestCase,
        required_tables: dict,
        soup: BeautifulSoup,
        dt_retrieved: datetime,
        runners_race_2: list[database.Runner],
    ) -> dict[str, Either]:
        race_status = required_tables["race_status"]
        runners = required_tables["runners"]
        race = required_tables["race"]
        odds = scraper.scrape_odds(race_status, soup, runners).bind(
            create_and_add(database.AmwagerIndividualOdds)
        )
        individual_pools = scraper.scrape_individual_pools(
            race_status, soup, runners, 1
        ).bind(create_and_add(database.IndividualPool))
        exotic_totals = scraper.scrape_exotic_totals(
            soup, race.id, 1, race_status
        ).bind(create_and_add(database.ExoticTotals))
        race_commissions = scraper.scrape_race_commissions(
            soup, race.id, 1, dt_retrieved
        ).bind(create_and_add(database.RaceCommission))
        exacta_odds = scraper.scrape_exacta_odds(soup, runners, race_status).bind(
            create_and_add(database.ExactaOdds)
        )
        double_odds = scraper.scrape_double_odds(
            soup, runners, runners_race_2, race_status
        ).bind(create_and_add(database.DoubleOdds))
        quinella_odds = scraper.scrape_quinella_odds(soup, runners, race_status).bind(
            create_and_add(database.QuinellaOdds)
        )
        willpays = scraper.scrape_willpays(soup, runners, 1, dt_retrieved).bind(
            create_and_add(database.WillpayPerDollar)
        )
        payouts = scraper.scrape_payouts(soup, 1, 1, dt_retrieved).bind(
            create_and_add(database.PayoutPerDollar)
        )
        return {
            "odds": odds,
            "individual_pools": individual_pools,
            "exotic_totals": exotic_totals,
            "race_commissions": race_commissions,
            "exacta_odds": exacta_odds,
            "double_odds": double_odds,
            "quinella_odds": quinella_odds,
            "willpays": willpays,
            "payouts": payouts,
        }

    def standard_test(
        self: unittest.TestCase,
        soup: BeautifulSoup,
        non_existant_tables: list[str],
        num_runners_next_race: int = 0,
    ):
        add_prep_models()
        dt_retrieved = datetime.now(ZoneInfo("UTC"))
        required_tables = self.scrape_race_status_runners(soup, dt_retrieved)
        runners_race_2 = create_next_race_runners(
            required_tables["race"], num_runners_next_race
        )
        dependent_tables = self.scrape_dependent_tables(
            required_tables, soup, dt_retrieved, runners_race_2
        )
        for key in dependent_tables:
            if key in non_existant_tables:
                self.assertTrue(dependent_tables[key].is_left() is True)
            else:
                self.assertTrue(dependent_tables[key].is_right() is True)

    def test_post_time_listed(self):
        # willpays has extra runner, possibly from a scratched runner
        self.standard_test(
            SOUPS["post_time_listed"],
            ["exacta_odds", "quinella_odds", "double_odds", "willpays", "payouts"],
        )

    def test_mtp_listed(self):
        self.standard_test(
            SOUPS["mtp_listed"], ["quinella_odds", "double_odds", "willpays", "payouts"]
        )

    def test_wagering_closed(self):
        # Unknown status of quinella odds, table references runners that do not exist
        # payouts has multiple of the same bet
        self.standard_test(
            SOUPS["wagering_closed"],
            ["quinella_odds", "payouts"],
            num_runners_next_race=7,
        )

    def test_results_posted(self):
        def _create_runners(*args, **kwargs):
            df = pandas.DataFrame(
                {"name": [], "morning_line": [], "tab": [], "race_id": []}
            )
            for x in range(1, 15):
                row = pandas.DataFrame(
                    {
                        "name": ["runner %s" % x],
                        "morning_line": ["1/9"],
                        "tab": [x],
                        "race_id": [1],
                    }
                )
                df = pandas.concat([df, row])
            return Right(df)

        scraper.scrape_runners = _create_runners
        # Has duplicate bet types in payouts
        # self.standard_test(
        #    SOUPS["results_posted"],
        #    ["quinella_odds", "payouts"],
        #    num_runners_next_race=12,
        # )
        self.standard_test(
            SOUPS["results_posted_modified_payouts"],
            ["quinella_odds"],
            num_runners_next_race=12,
        )
