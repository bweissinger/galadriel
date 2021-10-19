import logging
import re

from zoneinfo import ZoneInfo
from sqlalchemy.sql.expression import and_, null
from sqlalchemy.util.langhelpers import clsname_as_plain_name
from zoneinfo._common import ZoneInfoNotFoundError
from sqlalchemy.orm import declarative_base
from sqlalchemy import (
    event,
    exc,
    create_engine,
    Column,
    Integer,
    String,
    Float,
    ForeignKey,
    Date,
    DateTime,
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
from sqlite3 import Error as sql3_error
from sqlalchemy.schema import UniqueConstraint, CheckConstraint
from datetime import datetime, timedelta, date
from collections.abc import Iterable
from pandas import DataFrame
from pymonad.either import Either, Left, Right
from pymonad.tools import curry
from sqlalchemy.sql.elements import or_
from sqlalchemy.ext.declarative import declared_attr
from decimal import Decimal, InvalidOperation


logger = logging.getLogger(__name__)


def pascal_case_to_snake_case(string: str):
    return re.sub(r"(?<!^)(?=[A-Z])", "_", string).lower()


class BaseCls:
    @declared_attr
    def __tablename__(cls):
        return pascal_case_to_snake_case(cls.__name__)

    id = Column(Integer, primary_key=True)


Base = declarative_base(cls=BaseCls)


def setup_db(db_path: str = "sqlite:///:memory:") -> None:
    global engine, db_session, Base
    engine = create_engine(db_path)
    db_session = scoped_session(
        sessionmaker(
            autocommit=False, autoflush=False, bind=engine  # pragma: no mutate
        )
    )
    Base.query = db_session.query_property()
    Base.metadata.create_all(engine)


@event.listens_for(Engine, "connect")
def _set_sqlite_pragma(dbapi_connection, connection_record):
    if isinstance(dbapi_connection, SQL3Conn):
        cursor = dbapi_connection.cursor()
        cursor.execute("PRAGMA foreign_keys=ON;")
        cursor.close()


@curry(2)
def _integrity_check_failed(self, msg):
    raise exc.IntegrityError(msg, self.__dict__, self.__class__)


def are_of_same_race(runners: list[type["Runner"]]) -> Either[str, bool]:
    try:
        return Right(all(runner.race.id == runners[0].race.id for runner in runners))
    except TypeError as e:
        return Left("Unable to determine if runners are of same race: %s" % e)


# Will fail if they are not in order already
def are_consecutive_races(runners: list[type["Runner"]]) -> Either[str, bool]:
    try:
        previous = runners[0]
        for runner in runners[1:]:
            if not (
                (runner.race.meet_id == previous.race.meet_id)
                and (runner.race.race_num == previous.race.race_num + 1)
            ):
                return Right(False)
            previous = runner
    except (TypeError, IndexError) as e:
        return Left("Unable to check if races are consecutive: %s" % str(e))
    return Right(True)


def get_models_from_ids(
    ids: list[int], model: type["Base"]
) -> Either[str, type["Runner"]]:
    if not type(ids) == list:
        ids = [ids]
    result = [db_session.get(model, m_id) for m_id in ids]
    return (
        Right(result)
        if all(result)
        else Left("Unable to find all models with ids %s" % str(ids))
    )


def has_duplicates(models: list[type["Base"]]) -> Either[str, bool]:
    try:
        ids = [x.id for x in models]
        return Right(len(ids) != len(set(ids)))
    except (AttributeError, TypeError) as e:
        return Left("Error checking model duplication: %s" % e)


def add_and_commit(models: list[Base]) -> Either[str, Base]:
    if not isinstance(models, Iterable):
        models = [models]
    try:
        db_session.add_all(models)
        db_session.commit()
        return Right(models)
    except (exc.SQLAlchemyError, sql3_error) as e:
        db_session.rollback()
        return Left("Could not add to database: %s" % e)


def pandas_df_to_models(df: DataFrame, model: Base) -> Either[str, list[Base]]:
    try:
        dict_list = df.to_dict("records")
        return create_models_from_dict_list(dict_list, model)
    except AttributeError as e:
        return Left("Invalid dataframe: %s" % e)


def create_models_from_dict_list(
    vars: list[dict[str, object]], model: Base
) -> Either[str, list[Base]]:
    if not isinstance(vars, list):
        vars = [vars]
    try:
        return Right([model(**row) for row in vars])
    except (exc.SQLAlchemyError, TypeError) as e:
        return Left("Could not create model of type %s from %s: %s" % (model, vars, e))


@declarative_mixin  # pragma: no mutate
class DatetimeRetrievedMixin:

    datetime_retrieved = Column(DateTime, nullable=False)

    @validates("datetime_retrieved", include_backrefs=False)
    def validate_datetime_retrieved(self, key, datetime_retrieved):
        seconds = 10
        datetime_now = datetime.now(ZoneInfo("UTC"))
        td = timedelta(seconds=seconds)
        try:
            if datetime_retrieved.tzinfo != ZoneInfo("UTC"):
                _integrity_check_failed(self, "Datetime not UTC!")
            if datetime_retrieved > datetime_now:
                _integrity_check_failed(self, "Parsed datetime is in the future!")
            if datetime_retrieved < datetime_now - td:
                logger.warning(
                    "The parsed datetime is more than %s seconds "
                    "old! datetime_retrieved: %s, current utc datetime: %s"
                    % (seconds, datetime_retrieved, datetime_now)
                )
        except (AttributeError, TypeError) as e:
            _integrity_check_failed(self, "Invalid datetime: %s" % e)

        return datetime_retrieved


@declarative_mixin  # pragma: no mutate
class RaceStatusMixin(DatetimeRetrievedMixin):
    mtp = Column(Integer, CheckConstraint("mtp >= 0"), nullable=False)
    wagering_closed = Column(Boolean, nullable=False)
    results_posted = Column(Boolean, nullable=False)

    def _status_validation(self, wagering, results):
        if wagering is None or results is None:
            return
        if not wagering and results:
            _integrity_check_failed(
                self, "Wagering must be closed if results are posted!"
            )

    # Must check both, Depending on order of vars, one may not be set yet
    @validates("wagering_closed", include_backrefs=False)
    def validate_wagering_closed(self, key, wagering_closed):
        self._status_validation(wagering_closed, self.results_posted)
        return wagering_closed

    @validates("results_posted", include_backrefs=False)
    def validate_results_posted(self, key, results_posted):
        self._status_validation(self.wagering_closed, results_posted)
        return results_posted


@declarative_mixin  # pragma: no mutate
class TwoRunnerExoticOddsMixin(RaceStatusMixin):
    __table_args__ = (
        UniqueConstraint("runner_1_id", "runner_2_id", "datetime_retrieved"),
    )

    @declared_attr
    def runner_1_id(cls):
        return Column(Integer, ForeignKey("runner.id"), nullable=False)

    @declared_attr
    def runner_2_id(cls):
        return Column(Integer, ForeignKey("runner.id"), nullable=False)

    @declared_attr
    def platform_id(cls):
        return Column(Integer, ForeignKey("platform.id"), nullable=False)

    odds = Column(Float)

    @declared_attr
    def runner_1(cls):
        return relationship(
            "Runner",
            foreign_keys=[cls.runner_1_id],
            backref="%s_runner_1" % (pascal_case_to_snake_case(cls.__name__)),
        )

    @declared_attr
    def runner_2(cls):
        return relationship(
            "Runner",
            foreign_keys=[cls.runner_2_id],
            backref="%s_runner_2" % (pascal_case_to_snake_case(cls.__name__)),
        )


class Country(Base):
    name = Column(String, unique=True, nullable=False)
    amwager = Column(String, unique=True)
    twinspires = Column(String, unique=True)
    racing_and_sports = Column(String, unique=True)

    tracks = relationship("Track", backref="country")


class Track(Base):
    name = Column(String, unique=True, nullable=False)
    amwager = Column(String, unique=True)
    twinspires = Column(String, unique=True)
    racing_and_sports = Column(String, unique=True)
    country_id = Column(Integer, ForeignKey("country.id"), nullable=False)
    timezone = Column(String, nullable=False)

    meets = relationship("Meet", backref="track")

    @validates("timezone", include_backrefs=False)
    def validate_timezone(self, key, timezone):
        try:
            ZoneInfo(timezone)
            return timezone
        except (ZoneInfoNotFoundError, TypeError):
            _integrity_check_failed(self, "Not a valid timezone: %s" % timezone)


class Meet(Base, DatetimeRetrievedMixin):
    __table_args__ = (UniqueConstraint("track_id", "local_date"),)

    local_date = Column(Date, nullable=False)
    track_id = Column(Integer, ForeignKey("track.id"), nullable=False)

    races = relationship("Race", backref="meet")

    def _check_local_date(self, local_date, track_id):
        try:
            timezone = ZoneInfo(db_session.get(Track, track_id).timezone)
        except AttributeError as e:
            _integrity_check_failed(self, "Could not verify local_date: %s" % e)
        actual_date = datetime.now(ZoneInfo("UTC")).astimezone(timezone).date()
        if local_date != actual_date:
            logger.warning(
                "Meet date does not match the track's current date, "
                "is this correct? track_id: %s, local_date: %s, "
                "current local date: %s, utc datetime: %s"
                % (track_id, local_date, actual_date, datetime.now(ZoneInfo("UTC")))
            )

    @validates("track_id", include_backrefs=False)
    def validate_track_id(self, key, track_id):
        local_date = self.local_date
        if local_date is not None:
            self._check_local_date(local_date, track_id)
        return track_id

    @validates("local_date", include_backrefs=False)
    def validate_local_date(self, key, local_date):
        if type(local_date) != date:
            _integrity_check_failed(self, "Invalid date.")
        track_id = self.track_id
        if track_id:
            self._check_local_date(local_date, track_id)
        return local_date


class Race(Base, DatetimeRetrievedMixin):

    __table_args__ = (UniqueConstraint("meet_id", "race_num"),)

    race_num = Column(Integer, nullable=False)
    estimated_post = Column(DateTime, nullable=False)
    discipline_id = Column(Integer, ForeignKey("discipline.id"), nullable=False)
    meet_id = Column(Integer, ForeignKey("meet.id"), nullable=False)

    runners = relationship("Runner", backref="race")
    exotic_totals = relationship("ExoticTotals", backref="race")
    race_commissions = relationship("RaceCommission", backref="race")
    payouts_per_dollar = relationship("PayoutPerDollar", backref="race")

    def _meet_race_date_correct(self, meet_id, estimated_post):
        def _check_post_not_before_meet_date(meet):
            if meet.local_date > estimated_post.date():
                _integrity_check_failed(self, "Race estimated post before meet date!")

        if meet_id and estimated_post:
            get_models_from_ids(meet_id, Meet).either(
                lambda x: _integrity_check_failed(
                    self, "Could not find meet: %s" % str(x)
                ),
                lambda x: _check_post_not_before_meet_date(x[0]),
            )

    @validates("discipline_id", include_backrefs=False)
    def validate_discipline_id(self, key, discipline_id):
        if isinstance(discipline_id, int):
            return discipline_id
        elif isinstance(discipline_id, str):
            try:
                return (
                    Discipline.query.filter(
                        or_(
                            Discipline.name == discipline_id,
                            Discipline.amwager == discipline_id,
                        )
                    )
                    .first()
                    .id
                )
            except (exc.NoResultFound, AttributeError) as e:
                _integrity_check_failed(
                    self, "Cannot find discipline entry: %s" % str(e)
                )
        _integrity_check_failed(
            self, "Unknown type for discipline_id: %s" % str(discipline_id)
        )

    @validates("meet_id", include_backrefs=False)
    def validate_meet_id(self, key, meet_id):
        self._meet_race_date_correct(meet_id, self.estimated_post)
        return meet_id

    @validates("estimated_post", include_backrefs=False)
    def validate_estimated_post(self, key, estimated_post):
        datetime_now = datetime.now(ZoneInfo("UTC"))
        self._meet_race_date_correct(self.meet_id, estimated_post)
        try:
            if estimated_post < datetime_now:
                logger.warning(
                    "Estimated post appears to be in the past! "
                    "estimated_post: %s, current utc time: %s"
                    % (estimated_post, datetime_now)
                )
            elif estimated_post > datetime_now + timedelta(days=1):
                logger.warning(
                    "Estimated post appears to more than one day in the future!"
                    "estimated_post: %s, current utc time: %s"
                    % (estimated_post, datetime_now)
                )
        except TypeError:
            _integrity_check_failed(self, "Invalid datetime.")
        return estimated_post


class Runner(Base):

    __table_args__ = (UniqueConstraint("race_id", "tab"),)

    name = Column(String, nullable=False)
    age = Column(Integer, CheckConstraint("age > 0"))
    sex = Column(String)
    morning_line = Column(Float)
    tab = Column(Integer, CheckConstraint("tab > 0"), nullable=False)
    race_id = Column(Integer, ForeignKey("race.id"), nullable=False)
    result = Column(Integer, CheckConstraint("result > 0"))

    amwager_individual_odds = relationship("AmwagerIndividualOdds", backref="runner")
    racing_and_sports_runner_stats = relationship(
        "RacingAndSportsRunnerStat", backref="runner"
    )
    individual_pools = relationship("IndividualPool", backref="runner")
    willpays_per_dollar = relationship("WillpayPerDollar", backref="runner")


class AmwagerIndividualOdds(Base, RaceStatusMixin):

    __table_args__ = (UniqueConstraint("datetime_retrieved", "runner_id"),)

    runner_id = Column(Integer, ForeignKey("runner.id"), nullable=False)
    odds = Column(Float)
    tru_odds = Column(Float)


class RacingAndSportsRunnerStat(Base, DatetimeRetrievedMixin):

    runner_id = Column(Integer, ForeignKey("runner.id"), unique=True, nullable=False)
    form_3_starts = Column(String)
    form_5_starts = Column(String)
    weight = Column(String, CheckConstraint("weight > 0"))
    barrier_position = Column(Integer, CheckConstraint("barrier_position > 0"))
    barrier_position_adjusted = Column(
        Integer, CheckConstraint("barrier_position_adjusted > 0")
    )
    career_best = Column(String)
    season_best = Column(String)
    jockey_rating = Column(String)
    trainer_rating = Column(String)
    runs_this_campaign = Column(String)
    days_since_last_win = Column(Integer)
    runs_since_last_win = Column(Integer)
    days_since_last_run = Column(Integer)
    weight_change = Column(String)
    distance_change = Column(Integer)
    average_prize_money_career = Column(String)
    average_prize_money_12_months = Column(String)
    predicted_rating = Column(String)
    base_run_rating = Column(String)
    best_rating_12_months = Column(String)
    rating_good_to_fast = Column(String)
    rating_soft_to_heavy = Column(String)
    last_start_rating = Column(String)
    last_start_details = Column(String)
    ratings_50_days = Column(String)
    best_rating_last_3_runs = Column(String)
    api = Column(String)
    prepost_markets = Column(String)
    highest_winning_weight = Column(String)
    degree_of_difficulty = Column(String)
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
    final_rating = Column(String)
    theoretical_beaten_margin = Column(String)
    dividend = Column(String)
    speed_map_pace = Column(String)
    early_speed_figure = Column(String)
    final_speed_figure = Column(String)
    neural_algorithm_rating = Column(String)
    neural_algorithm_price = Column(String)


class IndividualPool(Base, RaceStatusMixin):
    __table_args__ = (UniqueConstraint("runner_id", "datetime_retrieved"),)

    runner_id = Column(Integer, ForeignKey("runner.id"), nullable=False)
    platform_id = Column(Integer, ForeignKey("platform.id"), nullable=False)
    win = Column(Integer, CheckConstraint("win >= 0"))
    place = Column(Integer, CheckConstraint("place >= 0"))
    show = Column(Integer, CheckConstraint("show >= 0"))


class DoubleOdds(Base, TwoRunnerExoticOddsMixin):
    @validates("runner_2_id")
    def validate_runner_ids(self, key, runner_2_id):
        def _is_valid(valid):
            if not valid:
                return Left("Runners not of consecutive races!")
            return Right(runner_2_id)

        runner_status = (
            get_models_from_ids([self.runner_1_id, runner_2_id], Runner)
            .bind(are_consecutive_races)
            .bind(_is_valid)
        )
        return runner_status.either(_integrity_check_failed(self), lambda x: x)


class ExactaOdds(Base, TwoRunnerExoticOddsMixin):
    @validates("runner_2_id")
    def validate_runner_ids(self, key, runner_2_id):
        @curry(2)
        def _compose_status(runners, duplicated):
            if duplicated:
                return Left("Duplicate runners!")

            def _same_race(valid):
                if not valid:
                    return Left("Runners of different races!")
                return Right(runner_2_id)

            return are_of_same_race(runners).bind(_same_race)

        runner_status = get_models_from_ids(
            [self.runner_1_id, runner_2_id], Runner
        ).bind(lambda x: has_duplicates(x).bind(_compose_status(x)))

        return runner_status.either(_integrity_check_failed(self), lambda x: x)


class QuinellaOdds(Base, TwoRunnerExoticOddsMixin):
    @validates("runner_2_id")
    def validate_runner_ids(self, key, runner_2_id):
        @curry(2)
        def _compose_status(runners, duplicated):
            if duplicated:
                return Left("Duplicate runners!")

            def _same_race(valid):
                if not valid:
                    return Left("Runners of different races!")
                return Right(runner_2_id)

            return are_of_same_race(runners).bind(_same_race)

        runner_status = get_models_from_ids(
            [self.runner_1_id, runner_2_id], Runner
        ).bind(lambda x: has_duplicates(x).bind(_compose_status(x)))

        return runner_status.either(_integrity_check_failed(self), lambda x: x)


class WillpayPerDollar(Base, DatetimeRetrievedMixin):

    runner_id = Column(Integer, ForeignKey("runner.id"), unique=True, nullable=False)
    platform_id = Column(Integer, ForeignKey("platform.id"), nullable=False)
    double = Column(Float, CheckConstraint("double >= 0"))
    pick_3 = Column(Float, CheckConstraint("pick_3 >= 0"))
    pick_4 = Column(Float, CheckConstraint("pick_4 >= 0"))
    pick_5 = Column(Float, CheckConstraint("pick_5 >= 0"))
    pick_6 = Column(Float, CheckConstraint("pick_6 >= 0"))


class Platform(Base):

    name = Column(String, unique=True, nullable=False)
    url = Column(String, unique=True)

    individual_pools = relationship("IndividualPool", backref="platform")
    double_odds = relationship("DoubleOdds", backref="platform")
    exacta_odds = relationship("ExactaOdds", backref="platform")
    quinella_odds = relationship("QuinellaOdds", backref="platform")
    willpays_per_dollar = relationship("WillpayPerDollar", backref="platform")
    exotic_totals = relationship("ExoticTotals", backref="platform")
    race_commissions = relationship("RaceCommission", backref="platform")
    payouts_per_dollar = relationship("PayoutPerDollar", backref="platform")


class Discipline(Base):

    name = Column(String, unique=True, nullable=False)
    amwager = Column(String, unique=True)

    races = relationship("Race", backref="discipline")


class ExoticTotals(Base, RaceStatusMixin):
    __table_args__ = (UniqueConstraint("race_id", "datetime_retrieved"),)

    race_id = Column(Integer, ForeignKey("race.id"), nullable=False)
    platform_id = Column(Integer, ForeignKey("platform.id"), nullable=False)
    exacta = Column(Integer, CheckConstraint("exacta >= 0"))
    quinella = Column(Integer, CheckConstraint("quinella >= 0"))
    trifecta = Column(Integer, CheckConstraint("trifecta >= 0"))
    superfecta = Column(Integer, CheckConstraint("superfecta >= 0"))
    double = Column(Integer, CheckConstraint("double >= 0"))
    pick_3 = Column(Integer, CheckConstraint("pick_3 >= 0"))
    pick_4 = Column(Integer, CheckConstraint("pick_4 >= 0"))
    pick_5 = Column(Integer, CheckConstraint("pick_5 >= 0"))
    pick_6 = Column(Integer, CheckConstraint("pick_6 >= 0"))


class RaceCommission(Base, DatetimeRetrievedMixin):

    race_id = Column(Integer, ForeignKey("race.id"), unique=True, nullable=False)
    platform_id = Column(Integer, ForeignKey("platform.id"), nullable=False)
    win = Column(Float, CheckConstraint("win >= 0 AND win <= 1"))
    place = Column(Float, CheckConstraint("place >= 0 AND place <= 1"))
    show = Column(Float, CheckConstraint("show >= 0 AND show <= 1"))
    exacta = Column(Float, CheckConstraint("exacta >= 0 AND exacta <= 1"))
    quinella = Column(Float, CheckConstraint("quinella >= 0 AND quinella <= 1"))
    trifecta = Column(Float, CheckConstraint("trifecta >= 0 AND trifecta <= 1"))
    superfecta = Column(Float, CheckConstraint("superfecta >= 0 AND superfecta <= 1"))
    double = Column(Float, CheckConstraint("double >= 0 AND double <= 1"))
    pick_3 = Column(Float, CheckConstraint("pick_3 >= 0 AND pick_3 <= 1"))
    pick_4 = Column(Float, CheckConstraint("pick_4 >= 0 AND pick_4 <= 1"))
    pick_5 = Column(Float, CheckConstraint("pick_5 >= 0 AND pick_5 <= 1"))
    pick_6 = Column(Float, CheckConstraint("pick_6 >= 0 AND pick_6 <= 1"))


class PayoutPerDollar(Base, DatetimeRetrievedMixin):

    race_id = Column(Integer, ForeignKey("race.id"), unique=True, nullable=False)
    platform_id = Column(Integer, ForeignKey("platform.id"), nullable=False)
    exacta = Column(Float, CheckConstraint("exacta >= 0"))
    quinella = Column(Float, CheckConstraint("quinella >= 0"))
    trifecta = Column(Float, CheckConstraint("trifecta >= 0"))
    superfecta = Column(Float, CheckConstraint("superfecta >= 0"))
    double = Column(Float, CheckConstraint("double >= 0"))
    pick_3 = Column(Float, CheckConstraint("pick_3 >= 0"))
    pick_4 = Column(Float, CheckConstraint("pick_4 >= 0"))
    pick_5 = Column(Float, CheckConstraint("pick_5 >= 0"))
    pick_6 = Column(Float, CheckConstraint("pick_6 >= 0"))
