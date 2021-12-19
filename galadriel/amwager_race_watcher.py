import time
import operator

from datetime import datetime
from bs4 import BeautifulSoup
from pymonad.tools import curry
from pymonad.either import Right
from typing import Dict

try:
    from zoneinfo import ZoneInfo
except ImportError:
    from backports.zoneinfo import ZoneInfo

from galadriel import database, amwager_scraper, amwager_watcher


class RaceWatcher(amwager_watcher.Watcher):
    @amwager_watcher.retry_with_timeout(10, 10)
    def _go_to_race(self, race_num, force_refresh=False) -> None:
        return super()._go_to_race(race_num, force_refresh=force_refresh)

    @curry(4)
    def _scrape_data(self, soup, datetime_retrieved, race_status):
        tables = [
            amwager_scraper.scrape_odds(race_status, soup, self.runners).bind(
                database.pandas_df_to_models(database.AmwagerIndividualOdds)
            ),
            amwager_scraper.scrape_individual_pools(
                race_status, soup, self.runners
            ).bind(database.pandas_df_to_models(database.IndividualPool)),
            amwager_scraper.scrape_exacta_odds(soup, self.runners, race_status).bind(
                database.pandas_df_to_models(database.ExactaOdds)
            ),
            amwager_scraper.scrape_quinella_odds(soup, self.runners, race_status).bind(
                database.pandas_df_to_models(database.QuinellaOdds)
            ),
            amwager_scraper.scrape_exotic_totals(soup, self.race.id, race_status).bind(
                database.pandas_df_to_models(database.ExoticTotals)
            ),
            amwager_scraper.scrape_willpays(
                soup, self.runners, datetime_retrieved
            ).bind(database.pandas_df_to_models(database.WillpayPerDollar)),
            amwager_scraper.scrape_race_commissions(
                soup, self.race.id, datetime_retrieved
            ).bind(database.pandas_df_to_models(database.RaceCommission)),
        ]
        if self.race_2_runners:
            tables.append(
                amwager_scraper.scrape_double_odds(
                    soup, self.runners, self.race_2_runners, race_status
                ).bind(database.pandas_df_to_models(database.DoubleOdds))
            )
        if race_status["results_posted"]:
            tables.append(
                amwager_scraper.scrape_payouts(soup, self.race.id, datetime_retrieved)
            )
        for table_list in tables:
            table_list.bind(database.add_and_commit(self.session))

    @curry(3)
    def _update_runners(self, soup, race_status):
        # If update fails then just return original runner objects
        old_runners = self.runners
        self.runners = amwager_scraper.update_scratched_status(soup, self.runners)
        if race_status["results_posted"]:
            self.runners = self.runners.bind(amwager_scraper.scrape_results(soup))
        self.runners = self.runners.bind(database.update_models(self.session)).either(
            lambda x: old_runners, lambda x: x
        )

    def _get_runners(self, soup):
        self.runners = (
            amwager_scraper.scrape_runners(soup, self.race.id)
            .bind(database.pandas_df_to_models)
            .bind(database.add_and_commit(self.session))
            .either(lambda x: None, lambda x: x)
        )

    def _get_next_race_and_runners(self):
        self.race_2 = next(
            (
                race
                for race in self.race.meet.races
                if race.race_num == self.race.race_num + 1
            ),
            None,
        )
        if self.race_2:
            self.race_2_runners = sorted(
                self.race_2.runners, key=operator.attrgetter("tab")
            )
        else:
            self.race_2_runners = None

    def _watch_race(self):
        def _check_race_status(race_status):
            if race_status["results_posted"] or (
                race_status["wagering_closed"] and not self.get_results
            ):
                self.terminate = True
            return Right(race_status)

        self.terminate = False
        while not self.terminate:
            datetime_retrieved = datetime.now(ZoneInfo("UTC"))
            soup = BeautifulSoup(self.driver.page_source, "lxml")
            if not self.runners:
                self._get_runners(soup)
            else:
                seconds_since_update = amwager_scraper.scrape_seconds_since_update(
                    soup
                ).either(lambda x: 60, lambda x: x)
                if seconds_since_update > 30:
                    self._go_to_race(self.race.race_num, force_refresh=True)
                    continue
                race_status = amwager_scraper.get_race_status(soup, datetime_retrieved)
                race_status.bind(_check_race_status).bind(
                    self._scrape_data(soup, datetime_retrieved)
                )
                race_status.bind(self._update_runners(soup))
            time.sleep(10)

    def _get_race_info(self):
        self.race = self.session.query(database.Race).get(self.race_id)
        if not self.race:
            raise ValueError("Could not find race with id %s" % self.race_id)
        self.runners = self.race.runners
        self.track = self.race.meet.track

    def run(self):
        try:
            self.session = database.Session()
            self._get_race_info()
            self._get_next_race_and_runners()
            self._prepare_domain()
            self._go_to_race(self.race.race_num)
            self._watch_race()
        except Exception:
            self.logger.exception(
                "Exception while watching race with ID %s" % self.race_id
            )
        self.session.close()
        self.driver.quit()

    def __init__(
        self, race_id: int, get_results: bool, cookies: Dict, log_path: str = ""
    ):
        super().__init__(cookies, log_path)
        self.race_id = race_id
        self.get_results = get_results
