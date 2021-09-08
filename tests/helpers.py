import pytz

from datetime import datetime, timedelta, date


def add_objects_to_db(database):
    dt_now = datetime.now(pytz.utc)
    models = []
    models.append(database.Country(name='country_1'))
    models.append(database.Track(name='track_1', country_id=1, timezone='UTC'))
    models.append(
        database.Meet(local_date=date.today(),
                      track_id=1,
                      datetime_retrieved=dt_now))
    models.append(
        database.Race(race_num=1,
                      estimated_post=dt_now + timedelta(minutes=10),
                      datetime_retrieved=dt_now,
                      meet_id=1))
    models.append(
        database.Race(race_num=2,
                      estimated_post=dt_now + timedelta(minutes=30),
                      datetime_retrieved=dt_now,
                      meet_id=1))
    models.append(database.Runner(name='a', tab=1, race_id=1))
    models.append(database.Runner(name='b', tab=2, race_id=1))
    models.append(database.Runner(name='c', tab=1, race_id=2))
    models.append(
        database.AmwagerOdds(datetime_retrieved=dt_now,
                             mtp=10,
                             wagering_closed=False,
                             results_posted=False,
                             runner_id=1))
    models.append(
        database.RacingAndSportsRunnerStat(datetime_retrieved=dt_now,
                                           runner_id=1))
    models.append(database.Platform(name='amw'))
    database.add_and_commit(models)
    models = []
    models.append(
        database.IndividualPool(datetime_retrieved=dt_now,
                                mtp=10,
                                wagering_closed=False,
                                results_posted=False,
                                runner_id=1,
                                platform_id=1))
    models.append(
        database.DoublePool(datetime_retrieved=dt_now,
                            mtp=10,
                            wagering_closed=False,
                            results_posted=False,
                            runner_1_id=1,
                            runner_2_id=3,
                            platform_id=1,
                            pool=0))
    models.append(
        database.ExactaPool(datetime_retrieved=dt_now,
                            mtp=10,
                            wagering_closed=False,
                            results_posted=False,
                            runner_1_id=1,
                            runner_2_id=2,
                            platform_id=1,
                            pool=0))
    models.append(
        database.QuinellaPool(datetime_retrieved=dt_now,
                              mtp=10,
                              wagering_closed=False,
                              results_posted=False,
                              runner_1_id=1,
                              runner_2_id=2,
                              pool=0,
                              platform_id=1))
    models.append(
        database.WillpayPerDollar(datetime_retrieved=dt_now,
                                  runner_id=1,
                                  platform_id=1))
    database.add_and_commit(models)

    # Add second meet and associated models
    meet = database.Meet(local_date=date.today() + timedelta(days=1),
                         datetime_retrieved=dt_now,
                         track_id=1)
    database.add_and_commit(meet)
    race = database.Race(race_num=2,
                         estimated_post=dt_now,
                         datetime_retrieved=dt_now,
                         meet_id=meet.id)
    database.add_and_commit(race)
    runner = database.Runner(name='d', tab=1, race_id=race.id)
    database.add_and_commit(runner)
    race2 = database.Race(race_num=3,
                          estimated_post=dt_now,
                          datetime_retrieved=dt_now,
                          meet_id=1)
    database.add_and_commit(race2)
    runner = database.Runner(name='e', tab=1, race_id=race2.id)
    database.add_and_commit(runner)
    return
