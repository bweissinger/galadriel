from zoneinfo import ZoneInfo
from datetime import datetime, timedelta


def add_objects_to_db(database):
    dt_now = datetime.now(ZoneInfo("UTC"))
    date_today_utc = dt_now.date()
    models = []
    models.append(database.Country(name="country_1"))
    models.append(database.Track(name="track_1", country_id=1, timezone="UTC"))
    database.add_and_commit(models)
    models = []
    models.append(
        database.Meet(local_date=date_today_utc, track_id=1, datetime_retrieved=dt_now)
    )
    database.add_and_commit(models)
    models = []
    database.add_and_commit(database.Discipline(name="Thoroughbred", amwager="Tbred"))
    models.append(
        database.Race(
            race_num=1,
            estimated_post=dt_now + timedelta(minutes=10),
            discipline_id=1,
            datetime_retrieved=dt_now,
            meet_id=1,
        )
    )
    models.append(
        database.Race(
            race_num=2,
            estimated_post=dt_now + timedelta(minutes=30),
            discipline_id=1,
            datetime_retrieved=dt_now,
            meet_id=1,
        )
    )
    models.append(database.Runner(name="a", morning_line="1/9", tab=1, race_id=1))
    models.append(database.Runner(name="b", morning_line="1/9", tab=2, race_id=1))
    models.append(database.Runner(name="c", morning_line="1/9", tab=1, race_id=2))
    models.append(
        database.AmwagerIndividualOdds(
            datetime_retrieved=dt_now,
            mtp=10,
            wagering_closed=False,
            results_posted=False,
            runner_id=1,
        )
    )
    models.append(
        database.RacingAndSportsRunnerStat(datetime_retrieved=dt_now, runner_id=1)
    )
    models.append(database.Platform(name="amw"))
    database.add_and_commit(models)
    models = []
    models.append(
        database.IndividualPool(
            datetime_retrieved=dt_now,
            mtp=10,
            wagering_closed=False,
            results_posted=False,
            runner_id=1,
            platform_id=1,
        )
    )
    models.append(
        database.DoubleOdds(
            datetime_retrieved=dt_now,
            mtp=10,
            wagering_closed=False,
            results_posted=False,
            runner_1_id=1,
            runner_2_id=3,
            platform_id=1,
            pool=0,
        )
    )
    models.append(
        database.ExactaOdds(
            datetime_retrieved=dt_now,
            mtp=10,
            wagering_closed=False,
            results_posted=False,
            runner_1_id=1,
            runner_2_id=2,
            platform_id=1,
            pool=0,
        )
    )
    models.append(
        database.QuinellaOdds(
            datetime_retrieved=dt_now,
            mtp=10,
            wagering_closed=False,
            results_posted=False,
            runner_1_id=1,
            runner_2_id=2,
            pool=0,
            platform_id=1,
        )
    )
    models.append(
        database.WillpayPerDollar(datetime_retrieved=dt_now, runner_id=1, platform_id=1)
    )
    database.add_and_commit(models)

    # Add second meet and associated models
    database.add_and_commit(
        database.Track(name="track_2", country_id=1, timezone="UTC")
    )
    meet = database.Meet(
        local_date=date_today_utc,
        datetime_retrieved=dt_now,
        track_id=2,
    )
    database.add_and_commit(meet)
    race = database.Race(
        race_num=2,
        estimated_post=dt_now + timedelta(minutes=10),
        discipline_id=1,
        datetime_retrieved=dt_now,
        meet_id=meet.id,
    )
    database.add_and_commit(race)
    runner = database.Runner(name="d", tab=1, morning_line="1/9", race_id=race.id)
    database.add_and_commit(runner)
    race2 = database.Race(
        race_num=3,
        estimated_post=dt_now + timedelta(minutes=10),
        discipline_id=1,
        datetime_retrieved=dt_now,
        meet_id=1,
    )
    database.add_and_commit(race2)
    runner = database.Runner(name="e", tab=1, morning_line="1/9", race_id=race2.id)
    database.add_and_commit(runner)
