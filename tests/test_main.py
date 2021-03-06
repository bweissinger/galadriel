import unittest

from datetime import datetime, timedelta
from freezegun import freeze_time

try:
    from zoneinfo import ZoneInfo
except ImportError:
    from backports.zoneinfo import ZoneInfo

from galadriel import database, __main__


class DBTestCase(unittest.TestCase):
    def setUp(self):
        super().setUp()
        database.setup_db("sqlite:///:memory:")
        self.session = database.Session()

    def tearDown(self):
        database.Base.metadata.drop_all(bind=database.engine)
        try:
            test_class = database.Base.metadata.tables["test_class"]
            database.Base.metadata.remove(test_class)
        except KeyError:
            pass
        self.session.close()
        return super().tearDown()


class TestGetTodaysMeetsInDatabase(DBTestCase):
    @freeze_time("2020-01-01 12:30:00")
    def setUp(self) -> None:
        super().setUp()
        dt = datetime.now(ZoneInfo("UTC"))
        database.add_and_commit(self.session, database.Country(name="a"))
        database.add_and_commit(
            self.session, database.Track(name="test", timezone="UTC", country_id=1)
        )
        database.add_and_commit(
            self.session,
            database.Track(name="test_2", timezone="UTC", country_id=1),
        )
        database.add_and_commit(
            self.session,
            database.Track(name="test_3", timezone="UTC", country_id=1),
        )
        database.add_and_commit(
            self.session,
            database.Meet(
                datetime_retrieved=dt,
                local_date=dt.date() - timedelta(days=1),
                track_id=1,
            ),
        )
        database.add_and_commit(
            self.session,
            database.Meet(datetime_retrieved=dt, local_date=dt.date(), track_id=1),
        )
        database.add_and_commit(
            self.session,
            database.Meet(
                datetime_retrieved=dt,
                local_date=dt.date() - timedelta(days=1),
                track_id=2,
            ),
        )
        database.add_and_commit(
            self.session,
            database.Meet(
                datetime_retrieved=dt,
                local_date=dt.date() + timedelta(days=1),
                track_id=3,
            ),
        )
        database.add_and_commit(
            self.session,
            database.Meet(
                datetime_retrieved=dt,
                local_date=dt.date(),
                track_id=3,
            ),
        )

    def tearDown(self) -> None:
        super().tearDown()

    @freeze_time("2020-01-01 12:30:00")
    def test_no_meets_today(self):
        database.delete_models(
            self.session,
            self.session.query(database.Meet).get(2),
        )
        database.delete_models(self.session, self.session.query(database.Meet).get(5))
        output = __main__._get_todays_meets_not_ignored(self.session)
        self.assertEqual(output, [])

    @freeze_time("2020-01-01 12:30:00")
    def test_some_tracks_have_meets_today(self):
        output = __main__._get_todays_meets_not_ignored(self.session)
        ids = [meet.id for meet in output]
        self.assertEqual(ids, [2, 5])

    @freeze_time("2020-01-01 12:30:00")
    def test_track_has_no_meets(self):
        database.delete_models(self.session, self.session.query(database.Meet).get(1))
        output = __main__._get_todays_meets_not_ignored(self.session)
        ids = [meet.id for meet in output]
        self.assertEqual(ids, [2, 5])


class TestGetTracksToScrape(DBTestCase):
    @freeze_time("2020-01-01 12:30:00")
    def setUp(self) -> None:
        super().setUp()
        database.add_and_commit(self.session, database.Country(name="a"))
        database.add_and_commit(
            self.session,
            database.Track(name="test", amwager="test", timezone="UTC", country_id=1),
        )
        database.add_and_commit(
            self.session,
            database.Track(
                name="test_2", amwager="test_2", timezone="UTC", country_id=1
            ),
        )

    def test_extra_meets_in_database(self):
        output = __main__._get_tracks_to_scrape(self.session, [{"id": "test"}])
        ids = [track.id for track in output]
        self.assertEqual(ids, [1])

    def test_extra_meets_in_listed(self):
        output = __main__._get_tracks_to_scrape(
            self.session, [{"id": "test"}, {"id": "test_2"}, {"id": "test_3"}]
        )
        ids = [track.id for track in output]
        self.assertEqual(ids, [1, 2])

    def test_empty_meets_listed(self):
        output = __main__._get_tracks_to_scrape(self.session, [])
        self.assertEqual(output, [])

    def test_empty_meets_in_database(self):
        database.delete_models(self.session, self.session.query(database.Track).all())
        output = __main__._get_tracks_to_scrape(self.session, [{"id": "test"}])
        self.assertEqual(output, [])

    def test_ignores_track(self):
        model = self.session.query(database.Track).first()
        model.ignore = True
        database.update_models(self.session, model)
        output = __main__._get_tracks_to_scrape(
            self.session, [{"id": "test"}, {"id": "test_2"}]
        )
        ids = [track.id for track in output]
        self.assertEqual(ids, [2])


class TestGetTodaysRacesWithoutResults(DBTestCase):
    @freeze_time("2020-01-01 12:30:00")
    def setUp(self) -> None:
        super().setUp()
        dt = datetime.now(ZoneInfo("UTC"))
        database.add_and_commit(
            self.session, database.Discipline(name="thoroughbred", amwager="Tbred")
        )
        database.add_and_commit(self.session, database.Country(name="a"))
        database.add_and_commit(
            self.session, database.Track(name="test", timezone="UTC", country_id=1)
        )
        database.add_and_commit(
            self.session,
            database.Meet(
                datetime_retrieved=dt,
                local_date=dt.date() - timedelta(days=1),
                track_id=1,
            ),
        )
        database.add_and_commit(
            self.session,
            database.Meet(datetime_retrieved=dt, local_date=dt.date(), track_id=1),
        )
        database.add_and_commit(
            self.session,
            database.Race(
                datetime_retrieved=dt,
                race_num=1,
                estimated_post=dt - timedelta(days=1),
                meet_id=1,
                discipline_id=1,
            ),
        )
        database.add_and_commit(
            self.session,
            database.Race(
                datetime_retrieved=dt,
                race_num=1,
                estimated_post=dt + timedelta(minutes=10),
                meet_id=2,
                discipline_id=1,
            ),
        )
        database.add_and_commit(
            self.session,
            database.Race(
                datetime_retrieved=dt,
                race_num=2,
                estimated_post=dt + timedelta(minutes=15),
                meet_id=2,
                discipline_id=1,
            ),
        )
        database.add_and_commit(
            self.session,
            database.Runner(name="horse_a", tab=1, race_id=1, scratched=False),
        )
        database.add_and_commit(
            self.session,
            database.Runner(
                name="horse_b", tab=1, race_id=2, scratched=False, result=True
            ),
        )
        database.add_and_commit(
            self.session,
            database.Runner(
                name="horse_c",
                tab=2,
                race_id=2,
                scratched=False,
            ),
        )
        database.add_and_commit(
            self.session,
            database.Runner(name="horse_d", tab=1, race_id=3, scratched=False),
        )

    @freeze_time("2020-01-01 12:30:00")
    def test_todays_races_without_results(self):
        output = __main__._get_todays_races_without_results(self.session)
        ids = [race.id for race in output]
        self.assertEqual(ids, [3])
