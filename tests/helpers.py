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
                      datetime_parsed_utc=dt_now))
    models.append(
        database.Race(race_num=1,
                      estimated_post_utc=dt_now + timedelta(minutes=10),
                      datetime_parsed_utc=dt_now,
                      meet_id=1))
    models.append(
        database.Race(race_num=2,
                      estimated_post_utc=dt_now + timedelta(minutes=30),
                      datetime_parsed_utc=dt_now,
                      meet_id=1))
    models.append(database.Horse(name='horse_1'))
    models.append(database.Horse(name='horse_2'))
    models.append(database.Horse(name='horse_3'))
    models.append(database.Jockey(name='jockey_1'))
    models.append(database.Trainer(name='trainer_1'))
    models.append(
        database.Runner(horse_id=1,
                        jockey_id=1,
                        trainer_id=1,
                        tab=1,
                        race_id=1))
    models.append(database.Runner(horse_id=2, tab=2, race_id=1))
    models.append(database.Runner(horse_id=3, tab=1, race_id=2))
    models.append(
        database.AmwagerOdds(datetime_parsed_utc=dt_now,
                             mtp=10,
                             is_post_race=False,
                             runner_id=1))
    models.append(
        database.RacingAndSportsRunnerStat(datetime_parsed_utc=dt_now,
                                           runner_id=1))
    models.append(database.Platform(name='amw'))
    database.add_and_commit(models)
    models = []
    models.append(
        database.IndividualPool(datetime_parsed_utc=dt_now,
                                mtp=10,
                                is_post_race=False,
                                runner_id=1,
                                platform_id=1))
    models.append(
        database.DoublePool(datetime_parsed_utc=dt_now,
                            mtp=10,
                            is_post_race=False,
                            runner_1_id=1,
                            runner_2_id=3,
                            platform_id=1,
                            pool=0))
    models.append(
        database.ExactaPool(datetime_parsed_utc=dt_now,
                            mtp=10,
                            is_post_race=False,
                            runner_1_id=1,
                            runner_2_id=2,
                            platform_id=1,
                            pool=0))
    models.append(
        database.QuinellaPool(datetime_parsed_utc=dt_now,
                              mtp=10,
                              is_post_race=False,
                              runner_1_id=1,
                              runner_2_id=2,
                              pool=0,
                              platform_id=1))
    models.append(
        database.WillpayPerDollar(datetime_parsed_utc=dt_now,
                                  runner_id=1,
                                  platform_id=1))
    database.add_and_commit(models)
    return