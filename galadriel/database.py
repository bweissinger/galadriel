import logging
import re
import os

from zoneinfo import ZoneInfo
from zoneinfo._common import ZoneInfoNotFoundError
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
from sqlalchemy import orm
from sqlalchemy.orm import (
    scoped_session,
    sessionmaker,
    relationship,
    validates,
    declarative_mixin,
    declarative_base,
    backref,
)
from sqlalchemy.engine import Engine
from sqlite3 import Connection as SQL3Conn
from sqlite3 import Error as sql3_error
from sqlalchemy.schema import UniqueConstraint, CheckConstraint
from datetime import datetime, timedelta, date
from collections.abc import Iterable
from pandas import DataFrame, Timestamp
from pymonad.either import Either, Left, Right
from pymonad.tools import curry
from sqlalchemy.sql.elements import or_
from sqlalchemy.ext.declarative import declared_attr


logger = logging.getLogger("DATABASE_LOGGER")


def pascal_case_to_snake_case(string: str):
    return re.sub(r"(?<!^)(?=[A-Z])", "_", string).lower()


class BaseCls:
    @declared_attr
    def __tablename__(cls):
        return pascal_case_to_snake_case(cls.__name__)

    id = Column(Integer, primary_key=True)


Base = declarative_base(cls=BaseCls)


def setup_db(db_path: str = "sqlite:///:memory:", log_path: str = "") -> None:
    global engine, Session, Base
    engine = create_engine(db_path)
    Session = scoped_session(
        sessionmaker(autocommit=False, autoflush=False, bind=engine)
    )
    # Base.query = db_session.query_property()
    Base.metadata.create_all(engine)

    fh = logging.FileHandler(os.path.join(log_path, "database.log"))
    formatter = logging.Formatter(
        "%(asctime)s | %(name)s | %(levelname)s | %(message)s"
    )
    fh.setFormatter(formatter)
    logger.addHandler(fh)


def close_db() -> None:
    engine.dispose()


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
    ids: list[int], model: type["Base"], session: scoped_session
) -> Either[str, type["Runner"]]:
    if not type(ids) == list:
        ids = [ids]
    result = [session.get(model, m_id) for m_id in ids]
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


def has_results(race: "Race") -> bool:
    for runner in race.runners:
        if runner.result:
            return True
    return False


def has_odds_or_stats(race: "Race") -> bool:
    for runner in race.runners:
        if (
            not runner.amwager_individual_odds
            and not runner.racing_and_sports_runner_stats
        ):
            return False
    return True


def add_and_commit(
    models: list[Base], session: scoped_session = None
) -> Either[str, list[Base]]:
    close_session = False
    if not isinstance(models, Iterable):
        models = [models]
    if not session:
        close_session = True
        session = Session()
    try:
        session.add_all(models)
        session.commit()
        return Right(models)
    except (exc.SQLAlchemyError, sql3_error) as e:
        session.rollback()
        return Left("Could not add to database: %s" % e)
    finally:
        if close_session:
            Session.remove()


@curry(2)
def update_models(
    session: scoped_session,
    models: list[Base],
) -> Either[str, list[Base]]:
    if not isinstance(models, Iterable):
        models = [models]
    try:
        session.commit()
        return Right(models)
    except (exc.SQLAlchemyError, sql3_error) as e:
        session.rollback()
        return Left("Could not update models: %s" % e)


def delete_models(session: scoped_session, models: list[Base]) -> Either[str, None]:
    if not isinstance(models, Iterable):
        models = [models]
    try:
        for model in models:
            session.delete(model)
        session.commit()
        return Right(None)
    except orm.exc.UnmappedInstanceError:
        return Left("Could not delete models.")


@curry(2)
def pandas_df_to_models(model: Base, df: DataFrame) -> Either[str, list[Base]]:
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
        seconds = 15
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

    odds = Column(Float)
    fair_value_odds = Column(Float)

    @declared_attr
    def runner_1(cls):
        return relationship(
            "Runner",
            foreign_keys=[cls.runner_1_id],
            backref=backref(
                "%s_runner_1" % (pascal_case_to_snake_case(cls.__name__)),
                cascade="all,delete",
            ),
        )

    @declared_attr
    def runner_2(cls):
        return relationship(
            "Runner",
            foreign_keys=[cls.runner_2_id],
            backref=backref(
                "%s_runner_2" % (pascal_case_to_snake_case(cls.__name__)),
                cascade="all,delete",
            ),
        )


class Country(Base):
    name = Column(String, unique=True, nullable=False)
    amwager = Column(String, unique=True)
    twinspires = Column(String, unique=True)
    racing_and_sports = Column(String, unique=True)

    tracks = relationship("Track", cascade="all,delete", backref="country")


class Track(Base):
    name = Column(String, unique=True, nullable=False)
    amwager = Column(String, unique=True)
    amwager_list_display = Column(String)
    twinspires = Column(String, unique=True)
    racing_and_sports = Column(String, unique=True)
    country_id = Column(Integer, ForeignKey("country.id"), nullable=False)
    timezone = Column(String, nullable=False)
    ignore = Column(Boolean, default=False)

    meets = relationship("Meet", cascade="all,delete", backref="track")

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

    races = relationship("Race", cascade="all,delete", backref="meet")

    def _check_local_date(self, local_date, track_id):
        session = Session()
        try:
            timezone = ZoneInfo(session.get(Track, track_id).timezone)
        except AttributeError as e:
            _integrity_check_failed(self, "Could not verify local_date: %s" % e)
        finally:
            Session.remove()

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
    estimated_post = Column(DateTime)
    discipline_id = Column(Integer, ForeignKey("discipline.id"), nullable=False)
    meet_id = Column(Integer, ForeignKey("meet.id"), nullable=False)

    runners = relationship("Runner", cascade="all,delete", backref="race")
    exotic_totals = relationship("ExoticTotals", cascade="all,delete", backref="race")
    race_commissions = relationship(
        "RaceCommission", cascade="all,delete", backref="race"
    )
    payouts_per_dollar = relationship(
        "PayoutPerDollar", cascade="all,delete", backref="race"
    )

    def _meet_race_date_correct(self, meet_id, estimated_post):
        session = Session()

        def _failed(msg):
            Session.remove()
            _integrity_check_failed(self, msg)

        def _check_post_on_meet_date(meet):
            tmp = estimated_post
            if type(estimated_post) is Timestamp:
                tmp = tmp.to_pydatetime()

            local_est_post_date = (
                tmp.replace(tzinfo=ZoneInfo("UTC"))
                .astimezone(ZoneInfo(meet.track.timezone))
                .date()
            )

            if meet.local_date != local_est_post_date:
                _failed("Race estimated post not on local meet date!")

        if meet_id and estimated_post:
            get_models_from_ids(meet_id, Meet, session).either(
                lambda x: _failed("Could not find meet: %s" % str(x)),
                lambda x: _check_post_on_meet_date(x[0]),
            )

        Session.remove()

    @validates("discipline_id", include_backrefs=False)
    def validate_discipline_id(self, key, discipline_id):
        if isinstance(discipline_id, int):
            return discipline_id
        elif isinstance(discipline_id, str):
            session = Session()
            try:
                id_found = (
                    session.query(Discipline)
                    .filter(
                        or_(
                            Discipline.name == discipline_id,
                            Discipline.amwager == discipline_id,
                        )
                    )
                    .first()
                    .id
                )
                return id_found
            except (exc.NoResultFound, AttributeError) as e:
                _integrity_check_failed(
                    self, "Cannot find discipline entry: %s" % str(e)
                )
            finally:
                Session.remove()
        _integrity_check_failed(
            self, "Unknown type for discipline_id: %s" % str(discipline_id)
        )

    @validates("meet_id", include_backrefs=False)
    def validate_meet_id(self, key, meet_id):
        self._meet_race_date_correct(meet_id, self.estimated_post)
        return meet_id

    @validates("estimated_post", include_backrefs=False)
    def validate_estimated_post(self, key, estimated_post):
        self._meet_race_date_correct(self.meet_id, estimated_post)
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
    scratched = Column(Boolean, nullable=False)

    amwager_individual_odds = relationship(
        "AmwagerIndividualOdds", cascade="all,delete", backref="runner"
    )
    racing_and_sports_runner_stats = relationship(
        "RacingAndSportsRunnerStat", cascade="all,delete", backref="runner"
    )
    individual_pools = relationship(
        "IndividualPool", cascade="all,delete", backref="runner"
    )
    willpays_per_dollar = relationship(
        "WillpayPerDollar", cascade="all,delete", backref="runner"
    )


class AmwagerIndividualOdds(Base, RaceStatusMixin):

    __table_args__ = (UniqueConstraint("datetime_retrieved", "runner_id"),)

    runner_id = Column(Integer, ForeignKey("runner.id"), nullable=False)
    odds = Column(Float)
    tru_odds = Column(Float)


class RacingAndSportsRunnerStat(Base, DatetimeRetrievedMixin):

    runner_id = Column(Integer, ForeignKey("runner.id"), unique=True, nullable=False)
    jockey = Column(String)
    trainer = Column(String)
    form_3_starts = Column(String)
    form_5_starts = Column(String)
    weight = Column(Float, CheckConstraint("weight > 0"))
    barrier_position = Column(Integer, CheckConstraint("barrier_position > 0"))
    barrier_position_adjusted = Column(
        Integer, CheckConstraint("barrier_position_adjusted > 0")
    )
    career_best = Column(Float)
    season_best = Column(Float)
    jockey_rating = Column(Float)
    trainer_rating = Column(Float)
    runs_this_campaign = Column(String)
    days_since_last_win = Column(String)
    runs_since_last_win = Column(Integer)
    days_since_last_run = Column(Integer)
    weight_change = Column(Float)
    age = Column(Integer)
    sex = Column(String)
    distance_change = Column(Integer)
    average_prize_money_career = Column(Float)
    average_prize_money_12_months = Column(Float)
    predicted_rating = Column(Float)
    base_run_rating = Column(Float)
    best_rating_12_months = Column(Float)
    rating_good_to_fast = Column(Float)
    rating_soft_to_heavy = Column(Float)
    last_start_rating = Column(String)
    last_start_details = Column(String)
    ratings_50_days = Column(Float)
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
    __table_args__ = (UniqueConstraint("runner_id", "datetime_retrieved"),)

    runner_id = Column(Integer, ForeignKey("runner.id"), nullable=False)
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

        session = Session()
        runner_status = (
            get_models_from_ids([self.runner_1_id, runner_2_id], Runner, session)
            .bind(are_consecutive_races)
            .bind(_is_valid)
        )
        Session.remove()

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

        session = Session()
        runner_status = get_models_from_ids(
            [self.runner_1_id, runner_2_id], Runner, session
        ).bind(lambda x: has_duplicates(x).bind(_compose_status(x)))
        Session.remove()

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

        session = Session()
        runner_status = get_models_from_ids(
            [self.runner_1_id, runner_2_id], Runner, session
        ).bind(lambda x: has_duplicates(x).bind(_compose_status(x)))
        Session.remove()

        return runner_status.either(_integrity_check_failed(self), lambda x: x)


class WillpayPerDollar(Base, DatetimeRetrievedMixin):

    runner_id = Column(Integer, ForeignKey("runner.id"), unique=True, nullable=False)
    double = Column(Float, CheckConstraint("double >= 0"))
    pick_3 = Column(Float, CheckConstraint("pick_3 >= 0"))
    pick_4 = Column(Float, CheckConstraint("pick_4 >= 0"))
    pick_5 = Column(Float, CheckConstraint("pick_5 >= 0"))
    pick_6 = Column(Float, CheckConstraint("pick_6 >= 0"))


class Discipline(Base):

    name = Column(String, unique=True, nullable=False)
    amwager = Column(String, unique=True)
    racing_and_sports = Column(String, unique=True)

    races = relationship("Race", cascade="all,delete", backref="discipline")


class ExoticTotals(Base, RaceStatusMixin):
    __table_args__ = (UniqueConstraint("race_id", "datetime_retrieved"),)

    race_id = Column(Integer, ForeignKey("race.id"), nullable=False)
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
    exacta = Column(Float, CheckConstraint("exacta >= 0"))
    quinella = Column(Float, CheckConstraint("quinella >= 0"))
    trifecta = Column(Float, CheckConstraint("trifecta >= 0"))
    superfecta = Column(Float, CheckConstraint("superfecta >= 0"))
    double = Column(Float, CheckConstraint("double >= 0"))
    pick_3 = Column(Float, CheckConstraint("pick_3 >= 0"))
    pick_4 = Column(Float, CheckConstraint("pick_4 >= 0"))
    pick_5 = Column(Float, CheckConstraint("pick_5 >= 0"))
    pick_6 = Column(Float, CheckConstraint("pick_6 >= 0"))
