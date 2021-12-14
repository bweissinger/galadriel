from bs4 import BeautifulSoup
from datetime import datetime

try:
    from zoneinfo import ZoneInfo
except ImportError:
    from backports.zoneinfo import ZoneInfo

from galadriel import (
    database,
    amwager_scraper,
    racing_and_sports_scraper,
    amwager_watcher,
)
from galadriel.amwager_watcher import retry_with_timeout


class MeetPrepper(amwager_watcher.Watcher):
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
                self.logger.error(
                    "Error while adding race and runners to meet with track id %s | %s"
                    % (self.meet.track.id, e),
                    exc_info=True,
                )
                continue

    def _add_rns_data(self):
        @retry_with_timeout(5, 30)
        def _scrape_data():
            result = (
                racing_and_sports_scraper.scrape_meet(self.meet)
                .bind(database.pandas_df_to_models(database.RacingAndSportsRunnerStat))
                .bind(database.add_and_commit(self.session))
                .either(lambda x: x, lambda x: x)
            )
            if type(result) == str:
                raise ValueError(result)
            return

        if self.track.racing_and_sports:
            try:
                self.session.refresh(self.meet)
                # racingandsports.com seems to only have custom data downloads
                #   for Tbred races
                if self.meet.races:
                    if self.meet.races[0].discipline.name == "Thoroughbred":
                        _scrape_data()
                    else:
                        return
            except Exception as e:
                self.logger.error(
                    "Could not get rns data for track id: %s | %s"
                    % (self.meet.track.id, e),
                    exc_info=True,
                )

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
            self.logger.exception(
                "Exception during prepping of meet for track_id %s" % self.track_id
            )
            try:
                database.delete_models(self.session, self.meet)
            except AttributeError:
                pass
        self.session.close()
        self.driver.quit()

    def __init__(self, track_id: int, cookies: str, log_path: str = "") -> None:
        super().__init__(cookies, log_path)
        self.track_id = track_id
