import logging
import pytz

from sqlalchemy.orm import declarative_base
from sqlalchemy import (
    event,
    create_engine,
    Column,
    Integer,
    String,
    ForeignKey,
    Date,
    DateTime,
    Float,
    Boolean,
)
from sqlalchemy.orm import (
    scoped_session,
    sessionmaker,
    relationship,
    validates,
    declarative_mixin,
)
from sqlalchemy.engine import Engine
from sqlite3 import Connection as SQL3Conn
from sqlalchemy.schema import UniqueConstraint, CheckConstraint
from datetime import datetime, timedelta
from collections.abc import Iterable
from pandas import DataFrame

logger = logging.getLogger(__name__)


@event.listens_for(Engine, "connect")
def _set_sqlite_pragma(dbapi_connection, connection_record):
    if isinstance(dbapi_connection, SQL3Conn):
        cursor = dbapi_connection.cursor()
        cursor.execute("PRAGMA foreign_keys=ON;")
        cursor.close()


def are_of_same_race(runners: list[type['Runner']]):
    try:
        if not isinstance(runners, Iterable):
            runners = [runners]
        if len(runners) == 0:
            raise ValueError
        return all(runner.race.id == runners[0].race.id for runner in runners)
    except Exception as e:
        logger.error(e)
        raise


def are_consecutive_races(runners: list[type['Runner']]) -> Boolean:
    try:
        previous = runners[0]
        for runner in runners[1:]:
            if not ((runner.race.meet_id == previous.race.meet_id) and
                    (runner.race.race_num == previous.race.race_num + 1)):
                raise ValueError
            previous = runner
    except Exception as e:
        logger.error(e)
        return False
    return True


def get_models_from_ids(ids: list[int], model: type['Base']) -> type['Runner']:
    models = []
    try:
        for model_id in ids:
            models.append(model.query.filter(model.id == model_id).one())
    except Exception as e:
        logger.error(e)
        return []
    return models


def has_duplicates(models: list[type['Base']]) -> Boolean:
    try:
        if not isinstance(models, Iterable):
            models = [models]
        ids = [x.id for x in models]
        return not len(ids) == len(set(ids))
    except Exception as e:
        logger.error(e)
        raise


class BaseCls:

    id = Column(Integer, primary_key=True)


engine = None
db_session = None
Base = declarative_base(cls=BaseCls)


def setup_db(db_path: str = 'sqlite:///:memory:') -> None:
    global engine, db_session, Base
    engine = create_engine(db_path)
    db_session = scoped_session(
        sessionmaker(autocommit=False, autoflush=False, bind=engine))
    Base.query = db_session.query_property()
    Base.metadata.create_all(engine)
    return


def add_and_commit(models: list[Base]) -> bool:
    if not isinstance(models, Iterable):
        models = [models]
    try:
        db_session.add_all(models)
        db_session.commit()
        return True
    except Exception as e:
        logger.error(e)
        db_session.rollback()
        return False


def pandas_df_to_models(df: DataFrame, model: Base) -> list[Base]:
    dict_list = df.to_dict('records')
    return create_models_from_dict_list(dict_list, model)


def create_models_from_dict_list(vars: list[dict[str, object]],
                                 model: Base) -> list[Base]:
    if not isinstance(vars, list):
        vars = [vars]
    return [model(**row) for row in vars]


@declarative_mixin
class DatetimeParsedUtcMixin:

    datetime_parsed_utc = Column(DateTime(timezone=True), nullable=False)

    @validates('datetime_parsed_utc', include_backrefs=False)
    def validate_datetime_parsed_utc(self, key, datetime_parsed_utc):
        seconds = 10
        datetime_now = datetime.now(pytz.utc)
        td = timedelta(seconds=seconds)
        try:
            if datetime_parsed_utc.tzinfo != pytz.utc:
                raise ValueError('Parsed datetime is not UTC! '
                                 'datetime_parsed_utc: %s' %
                                 datetime_parsed_utc)
            if datetime_parsed_utc > datetime_now:
                raise ValueError(
                    'Parsed datetime appears to be in the future! '
                    'datetime_parsed_utc: %s, current utc '
                    'datetime: %s' % (datetime_parsed_utc, datetime_now))
            if datetime_parsed_utc < datetime_now - td:
                logger.warning(
                    'The parsed datetime is more than %s seconds '
                    'old! datetime_parsed_utc: %s, current utc datetime: %s' %
                    (seconds, datetime_parsed_utc, datetime_now))
        except (AttributeError, ValueError) as e:
            logger.error('Error validating datetime_parsed_utc : %s' % e)
            return None

        return datetime_parsed_utc


@declarative_mixin
class RaceStatusMixin(DatetimeParsedUtcMixin):
    mtp = Column(Integer, CheckConstraint('mtp >= 0'), nullable=False)
    is_post_race = Column(Boolean, nullable=False)


class Country(Base):
    __tablename__ = 'country'

    name = Column(String, unique=True, nullable=False)
    amwager = Column(String, unique=True)
    twinspires = Column(String, unique=True)
    racing_and_sports = Column(String, unique=True)

    tracks = relationship('Track', backref='country')


class Track(Base):
    __tablename__ = 'track'

    name = Column(String, unique=True, nullable=False)
    amwager = Column(String, unique=True)
    twinspires = Column(String, unique=True)
    racing_and_sports = Column(String, unique=True)
    country_id = Column(Integer, ForeignKey('country.id'), nullable=False)
    timezone = Column(String, nullable=False)

    meets = relationship('Meet', backref='track')

    @validates('timezone', include_backrefs=False)
    def validate_timezone(self, key, timezone):
        try:
            if timezone not in pytz.all_timezones:
                raise ValueError
            return timezone
        except ValueError:
            logger.error(str(timezone) + ' is not a valid pytz timezone!')
            return None


class Meet(Base, DatetimeParsedUtcMixin):
    __tablename__ = 'meet'
    __table_args__ = (UniqueConstraint('track_id', 'local_date'), )

    local_date = Column(Date, nullable=False)
    track_id = Column(Integer, ForeignKey('track.id'), nullable=False)

    races = relationship('Race', backref='meet')

    @validates('local_date', include_backrefs=False)
    def validate_local_date(self, key, local_date):
        date = datetime.utcnow().date()
        td = timedelta(days=1)
        try:
            assert (local_date >= date - td) and (local_date <= date + td)
        except AssertionError:
            logger.warning(
                'Local date is more than 1 day from the UTC date, '
                'is this correct? local_date: %s, current utc datetime: %s.' %
                (local_date, date))
        return local_date


class Race(Base, DatetimeParsedUtcMixin):
    __tablename__ = 'race'
    __table_args__ = (UniqueConstraint('meet_id', 'race_num'), )

    race_num = Column(Integer, nullable=False)
    estimated_post_utc = Column(DateTime, nullable=False)
    meet_id = Column(Integer, ForeignKey('meet.id'), nullable=False)

    runners = relationship('Runner', backref='race')

    @validates('estimated_post_utc', include_backrefs=False)
    def validate_estimated_post_utc(self, key, estimated_post_utc):
        datetime_now = datetime.now(pytz.utc)
        try:
            assert estimated_post_utc >= datetime_now
        except BaseException:
            logger.warning('Estimated post appears to be in the past! '
                           'estimated_post_utc: %s, current utc time: %s' %
                           (estimated_post_utc, datetime_now))
        return estimated_post_utc


class Runner(Base):
    __tablename__ = 'runner'
    __table_args__ = (UniqueConstraint('race_id', 'tab'), )

    name = Column(String, nullable=False)
    age = Column(Integer, CheckConstraint('age > 0'))
    sex = Column(String)
    tab = Column(Integer, CheckConstraint('tab > 0'), nullable=False)
    race_id = Column(Integer, ForeignKey('race.id'), nullable=False)

    amwager_odds = relationship('AmwagerOdds', backref='runner')
    racing_and_sports_runner_stats = relationship('RacingAndSportsRunnerStat',
                                                  backref='runner')
    individual_pools = relationship('IndividualPool', backref='runner')
    willpays_per_dollar = relationship('WillpayPerDollar', backref='runner')


class AmwagerOdds(Base, RaceStatusMixin):
    __tablename__ = 'amwager_odds'
    __table_args__ = (UniqueConstraint('datetime_parsed_utc', 'runner_id'), )

    runner_id = Column(Integer, ForeignKey('runner.id'), nullable=False)
    odds = Column(Float, CheckConstraint('odds > 0'))
    morning_line = Column(Float, CheckConstraint('morning_line > 0'))
    tru_odds = Column(Float, CheckConstraint('tru_odds > 0'))


class RacingAndSportsRunnerStat(Base, DatetimeParsedUtcMixin):
    __tablename__ = 'racing_and_sports_runner_stat'

    runner_id = Column(Integer,
                       ForeignKey('runner.id'),
                       unique=True,
                       nullable=False)
    form_3_starts = Column(String)
    form_5_starts = Column(String)
    weight = Column(Float, CheckConstraint('weight > 0'))
    barrier_position = Column(Integer, CheckConstraint('barrier_position > 0'))
    barrier_position_adjusted = Column(
        Integer, CheckConstraint('barrier_position_adjusted > 0'))
    career_best = Column(Float)
    season_best = Column(Float)
    jockey_rating = Column(Float)
    trainer_rating = Column(Float)
    runs_this_campaign = Column(String)
    days_since_last_win = Column(Integer)
    runs_since_last_win = Column(Integer)
    days_since_last_run = Column(Integer)
    weight_change = Column(Float)
    distance_change = Column(Integer)
    average_prize_money_career = Column(String)
    average_prize_money_12_months = Column(String)
    predicted_rating = Column(Float)
    base_run_rating = Column(Float)
    best_rating_12_months = Column(Float)
    rating_good_to_fast = Column(Float)
    rating_soft_to_heavy = Column(Float)
    last_start_rating = Column(Float)
    last_start_details = Column(String)
    ratings_50_days = Column(String)
    best_rating_last_3_runs = Column(Float)
    api = Column(Float)
    prepost_markets = Column(Float)
    highest_winning_weight = Column(Float)
    degree_of_difficulty = Column(Float)
    wps_jockey_and_horse = Column(String)
    wps_percent_jockey_and_horse = Column(String)
    wps_career = Column(String)
    wps_percent_career = Column(String)
    wps_12_month = Column(String)
    wps_percent_12_month = Column(String)
    wps_course = Column(String)
    wps_percent_course = Column(String)
    wps_course_and_distance = Column(String)
    wps_percent_course_and_distance = Column(String)
    wps_distance = Column(String)
    wps_percent_distance = Column(String)
    wps_fast = Column(String)
    wps_percent_fast = Column(String)
    wps_good_to_dead = Column(String)
    wps_percent_good_to_dead = Column(String)
    wps_soft_to_heavy = Column(String)
    wps_percent_soft_to_heavy = Column(String)
    wps_all_weather = Column(String)
    wps_percent_all_weather = Column(String)
    wps_turf = Column(String)
    wps_percent_turf = Column(String)
    wps_group_1 = Column(String)
    wps_percent_group_1 = Column(String)
    wps_group_2 = Column(String)
    wps_percent_group_2 = Column(String)
    wps_group_3 = Column(String)
    wps_percent_group_3 = Column(String)
    wps_listed_race = Column(String)
    wps_percent_listed_race = Column(String)
    wps_first_up = Column(String)
    wps_percent_first_up = Column(String)
    wps_second_up = Column(String)
    wps_percent_second_up = Column(String)
    wps_third_up = Column(String)
    wps_percent_third_up = Column(String)
    wps_clockwise = Column(String)
    wps_percent_clockwise = Column(String)
    wps_anti_clockwise = Column(String)
    wps_percent_anti_clockwise = Column(String)
    final_rating = Column(Float)
    theoretical_beaten_margin = Column(Float)
    dividend = Column(Float)
    speed_map_pace = Column(String)
    early_speed_figure = Column(Float)
    final_speed_figure = Column(Float)
    neural_algorithm_rating = Column(Float)
    neural_algorithm_price = Column(Float)


class IndividualPool(Base, RaceStatusMixin):
    __tablename__ = 'individual_pool'
    __table_args__ = (UniqueConstraint('runner_id', 'datetime_parsed_utc'), )

    runner_id = Column(Integer, ForeignKey('runner.id'), nullable=False)
    platform_id = Column(Integer, ForeignKey('platform.id'), nullable=False)
    win = Column(Integer, CheckConstraint('win >= 0'))
    place = Column(Integer, CheckConstraint('place >= 0'))
    show = Column(Integer, CheckConstraint('show >= 0'))
    one_dollar_payout = Column(Integer,
                               CheckConstraint('one_dollar_payout > 0'))


class DoublePool(Base, RaceStatusMixin):
    __tablename__ = 'double_pool'
    __table_args__ = (UniqueConstraint('runner_1_id', 'runner_2_id',
                                       'datetime_parsed_utc'), )

    runner_1_id = Column(Integer, ForeignKey('runner.id'), nullable=False)
    runner_2_id = Column(Integer, ForeignKey('runner.id'), nullable=False)
    platform_id = Column(Integer, ForeignKey('platform.id'), nullable=False)
    pool = Column(Integer, CheckConstraint('pool >= 0'), nullable=False)

    runner_1 = relationship(
        'Runner',
        foreign_keys=[runner_1_id],
        backref='double_pool_runner_1',
    )
    runner_2 = relationship(
        'Runner',
        foreign_keys=[runner_2_id],
        backref='double_pool_runner_2',
    )

    @validates('runner_2_id')
    def validate_runner_ids(self, key, runner_2_id):
        # Runner relationships not set yet
        runners = get_models_from_ids([self.runner_1_id, runner_2_id], Runner)

        try:
            if not are_consecutive_races(runners):
                logger.error('DoublePool: Runners not of consecutive races! '
                             'runner_1_id: %s, runner_2_id: %s' %
                             (self.runner_1_id, runner_2_id))
                raise ValueError
        except ValueError:
            return None

        return runner_2_id


class ExactaPool(Base, RaceStatusMixin):
    __tablename__ = 'exacta_pool'
    __table_args__ = (UniqueConstraint('runner_1_id', 'runner_2_id',
                                       'datetime_parsed_utc'), )

    runner_1_id = Column(Integer, ForeignKey('runner.id'), nullable=False)
    runner_2_id = Column(Integer, ForeignKey('runner.id'), nullable=False)
    platform_id = Column(Integer, ForeignKey('platform.id'), nullable=False)
    pool = Column(Integer, CheckConstraint('pool >= 0'), nullable=False)

    runner_1 = relationship(
        'Runner',
        foreign_keys=[runner_1_id],
        backref='exacta_pool_runner_1',
    )
    runner_2 = relationship(
        'Runner',
        foreign_keys=[runner_2_id],
        backref='exacta_pool_runner_2',
    )

    @validates('runner_2_id')
    def validate_runner_ids(self, key, runner_2_id):
        # Runner relationships not set yet
        runners = get_models_from_ids([self.runner_1_id, runner_2_id], Runner)
        try:
            if has_duplicates(runners):
                logger.error(
                    'ExactaPool: Runners are the same! runner_1_id: %s, '
                    'runner_2_id: %s' % (self.runner_1_id, runner_2_id))
                raise ValueError

            if not are_of_same_race(runners):
                logger.error(
                    'ExactaPool: Runners not of same race! runner_1_id: %s, '
                    'runner_2_id: %s' % (self.runner_1_id, runner_2_id))
                raise ValueError
        except ValueError:
            return None

        return runner_2_id


class QuinellaPool(Base, RaceStatusMixin):
    __tablename__ = 'quinella_pool'
    __table_args__ = (UniqueConstraint('runner_1_id', 'runner_2_id',
                                       'datetime_parsed_utc'), )

    runner_1_id = Column(Integer, ForeignKey('runner.id'), nullable=False)
    runner_2_id = Column(Integer, ForeignKey('runner.id'), nullable=False)
    platform_id = Column(Integer, ForeignKey('platform.id'), nullable=False)
    pool = Column(Integer, CheckConstraint('pool >= 0'), nullable=False)

    runner_1 = relationship(
        'Runner',
        foreign_keys=[runner_1_id],
        backref='quinella_pool_runner_1',
    )
    runner_2 = relationship(
        'Runner',
        foreign_keys=[runner_2_id],
        backref='quinella_pool_runner_2',
    )

    @validates('runner_2_id')
    def validate_runner_ids(self, key, runner_2_id):
        # Runner relationships not set yet
        runners = get_models_from_ids([self.runner_1_id, runner_2_id], Runner)
        try:
            if has_duplicates(runners):
                logger.error(
                    'QuinellaPool: Runners are the same! runner_1_id: %s, '
                    'runner_2_id: %s' % (self.runner_1_id, runner_2_id))
                raise ValueError

            if not are_of_same_race(runners):
                logger.error(
                    'QuinellaPool: Runners not of same race! runner_1_id: %s, '
                    'runner_2_id: %s' % (self.runner_1_id, runner_2_id))
                raise ValueError
        except ValueError:
            return None

        return runner_2_id


class WillpayPerDollar(Base, DatetimeParsedUtcMixin):
    __tablename__ = 'willpay_per_dollar'

    runner_id = Column(Integer,
                       ForeignKey('runner.id'),
                       unique=True,
                       nullable=False)
    platform_id = Column(Integer, ForeignKey('platform.id'), nullable=False)
    double = Column(Integer, CheckConstraint('double >= 0'))
    pick_3 = Column(Integer, CheckConstraint('pick_3 >= 0'))
    pick_4 = Column(Integer, CheckConstraint('pick_4 >= 0'))
    pick_5 = Column(Integer, CheckConstraint('pick_5 >= 0'))
    pick_6 = Column(Integer, CheckConstraint('pick_6 >= 0'))


class Platform(Base):
    __tablename__ = 'platform'

    name = Column(String, unique=True, nullable=False)
    url = Column(String, unique=True)

    individual_pools = relationship('IndividualPool', backref='platform')
    double_pools = relationship('DoublePool', backref='platform')
    exacta_pools = relationship('ExactaPool', backref='platform')
    quinella_pools = relationship('QuinellaPool', backref='platform')
    willpays_per_dollar = relationship('WillpayPerDollar', backref='platform')
