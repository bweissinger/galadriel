import re
import pandas
import operator

from bs4 import BeautifulSoup
from datetime import datetime, timedelta
from tzlocal import get_localzone
from pymonad.either import Left, Right, Either
from pymonad.tools import curry
from zoneinfo import ZoneInfo

from galadriel import resources
from galadriel.database import Runner


def _map_dataframe_table_names(
    df: pandas.DataFrame, alias: str
) -> Either[str, pandas.DataFrame]:

    try:
        columns = resources.get_table_map(alias)
        if len(columns) != len(df.columns):
            return Left(
                "Unable to map names: dataframe does not have correct number of columns"
            )
        df = df.rename(errors="raise", columns=columns)
        return Right(df)
    except (AttributeError, KeyError) as e:
        return Left("Unable to map names: %s" % e)


def _get_table(
    soup: BeautifulSoup, table_alias: str, table_attrs: dict[str, str]
) -> Either[str, pandas.DataFrame]:

    search = soup.find("table", table_attrs)
    try:
        table = pandas.read_html(str(search))[0]
        return _map_dataframe_table_names(table, table_alias)
    except ValueError:
        return Left("Unable to find table %s" % table_alias)


@curry(2)
def _add_runner_id_by_tab(
    runners: list[Runner], data_frame: pandas.DataFrame
) -> Either[str, pandas.DataFrame]:

    try:
        runners = sorted(runners, key=operator.attrgetter("tab"))
    except (AttributeError, TypeError) as e:
        return Left("Unable to add runner ids to DataFrame: %s" % e)

    ids = [runner.id for runner in runners]

    try:
        data_frame = data_frame.assign(runner_id=ids)
        return Right(data_frame)
    except ValueError as e:
        return Left("Unable to add runner ids to DataFrame: %s" % e)


# Results table is filled out with incorrect information when not visible
# When visible it has the 'runner runner-details-close' row
def _results_visible(soup: BeautifulSoup) -> bool:
    table = soup.find("table", {"class": "table table-Result table-Result-main"})
    try:
        rows = table.find_all("td", {"class": "runner runner-details-close"})
        if rows:
            return True
    except AttributeError:
        pass
    return False


def _get_results_posted_status(soup: BeautifulSoup) -> Either[str, bool]:

    runners = _get_table(soup, "amw_runners", {"id": "runner-view-inner-table"})

    # Problem if both exist or neither exist
    if _results_visible(soup):
        return runners.either(
            lambda y: Right(True),
            lambda y: Left("Unknown state, both runners and results tables exist."),
        )
    else:
        return runners.either(
            lambda y: Left("Unknown state, neither runners or results tables exist."),
            lambda y: Right(False),
        )


def _get_wagering_closed_status(soup: BeautifulSoup) -> Either[str, bool]:
    def _search(div):
        try:
            style = div["style"]
            if style == "display: none;":
                return Right(False)
            elif style == "":
                return Right(True)
            return Left("Unknown formatting: %s" % style)
        except (TypeError, KeyError) as e:
            return Left(str(e))

    div = soup.find("div", {"data-translate-lang": "wager.raceclosedmessage"})
    return _search(div).either(
        lambda x: Left("Cannot determine wagering status: %s" % x), Right
    )


def get_discipline(soup: BeautifulSoup) -> Either[str, str]:
    search = soup.find("li", {"class": "track_type"})
    try:
        return Right(search.text)
    except AttributeError as e:
        return Left("Cannot find race discipline: %s" % str(e))


def get_mtp(soup: BeautifulSoup, datetime_retrieved: datetime) -> Either[str, int]:
    def _get_mtp_text(m_soup):
        search = m_soup.find("span", {"class": "time"})
        try:
            return Right(search.text)
        except AttributeError:
            return Left("Could not find time on page.")

    def _get_int(text):
        try:
            return Right(int(text))
        except ValueError:
            return Left(text)

    def _get_post_time(text):
        try:
            post_time = datetime.strptime(text, "%I:%M %p")
        except ValueError:
            try:
                post_time = datetime.strptime(text, "%H:%M")
            except ValueError:
                return Left("Unknown time format: %s" % text)
        tz = get_localzone()
        local_date = datetime_retrieved.astimezone(tz).date()
        post = datetime.combine(local_date, post_time.time(), tzinfo=tz)
        return Right(post.astimezone(ZoneInfo("UTC")))

    def _post_time_to_mtp(post):
        if datetime_retrieved >= post:
            post += timedelta(days=1)
        return Right(int((post - datetime_retrieved).total_seconds() / 60))

    def _convert_to_mtp(text):
        return _get_post_time(text).bind(_post_time_to_mtp)

    return _get_mtp_text(soup).either(
        Left, lambda x: _get_int(x).either(_convert_to_mtp, Right)
    )


def get_race_status(
    soup: BeautifulSoup, datetime_retrieved: datetime
) -> Either[str, dict[str, object]]:
    def _add(func, key, params, m_dict):
        tmp = func(*params)
        return tmp.either(
            lambda x: Left("Cannot obtain race status: %s" % str(x)),
            lambda x: Right(m_dict | {key: x}),
        )

    def _add_mtp(m_dict):
        return _add(get_mtp, "mtp", [soup, datetime_retrieved], m_dict)

    def _add_results(m_dict):
        return _add(_get_results_posted_status, "results_posted", [soup], m_dict)

    def _add_wagering(m_dict):
        if m_dict["results_posted"]:
            out = Right(m_dict | {"wagering_closed": True})
        else:
            out = _add(_get_wagering_closed_status, "wagering_closed", [soup], m_dict)
        return out

    status = Right({"datetime_retrieved": datetime_retrieved})
    return status.bind(_add_mtp).bind(_add_results).bind(_add_wagering)


def get_track_list(soup: BeautifulSoup) -> Either[str, list[dict[str, str]]]:
    def _search(soup):
        search_re = re.compile("event_selector event-status*")
        races = soup.find_all("a", {"class": search_re})
        if len(races) == 0:
            return Left("Could not find track list in page.")
        return Right(races)

    def _get_dicts(race_list):
        try:
            return Right([{"id": race["id"], "html": str(race)} for race in race_list])
        except KeyError:
            return Left("Unknown formatting in race list.")

    return _search(soup).bind(_get_dicts)


def get_num_races(soup: BeautifulSoup) -> Either[str, int]:
    try:
        search = soup.find_all("button", {"id": re.compile("race-*")})
        nums = [int(x.text.rstrip()) for x in search if x.text != "All"]
        return Right(max(nums))
    except ValueError as e:
        return Left("Could not find the race numbers for this race. %s" % e)


def get_focused_race_num(soup: BeautifulSoup) -> Either[str, int]:
    search = soup.find("button", {"class": re.compile(r"r*track-num-fucus")})
    try:
        return Right(int(search.text))
    except (AttributeError, ValueError) as e:
        return Left("Unknown race focus status: %s" % str(e))


def scrape_race(
    soup: BeautifulSoup, datetime_retrieved: datetime, meet_id: int
) -> Either[str, pandas.DataFrame]:
    @curry(2)
    def _add_race_num(soup, df):
        race_num = get_focused_race_num(soup)
        return race_num.bind(lambda x: Right(df.assign(race_num=[x])))

    @curry(3)
    def _add_est_post(soup, datetime_retrieved, df):
        mtp = get_mtp(soup, datetime_retrieved)
        return mtp.bind(
            lambda x: Right(
                {"estimated_post": [datetime_retrieved + timedelta(minutes=x)]}
            )
        ).bind(lambda x: Right(df.assign(**x)))

    @curry(2)
    def _add_dt_retrieved(datetime_retrieved, df):
        return Right(df.assign(datetime_retrieved=[datetime_retrieved]))

    @curry(2)
    def _add_meet_id(meet_id, df):
        return Right(df.assign(**{"meet_id": [meet_id]}))

    @curry(2)
    def _add_discipline(soup, df):
        return get_discipline(soup).bind(
            lambda x: Right(df.assign(**{"discipline": [x]}))
        )

    return (
        Right(pandas.DataFrame())
        .bind(_add_race_num(soup))
        .bind(_add_est_post(soup, datetime_retrieved))
        .bind(_add_dt_retrieved(datetime_retrieved))
        .bind(_add_meet_id(meet_id))
        .bind(_add_discipline(soup))
        .either(lambda x: Left("Cannot scrape race: %s" % x), Right)
    )


def scrape_runners(soup: BeautifulSoup, race_id: int) -> Either[str, pandas.DataFrame]:
    def _transform_table(df):
        df = df[["name", "tab", "morning_line"]]
        df = df.assign(race_id=race_id)
        return Right(df)

    runners_table = _get_table(
        soup, "amw_runners", {"id": "runner-view-inner-table"}
    ).either(lambda x: Left("Cannot scrape runners: %s" % x), Right)

    return runners_table.bind(_transform_table)


def scrape_odds(
    race_status: dict,
    soup: BeautifulSoup,
    runners: list[Runner],
) -> Either[str, pandas.DataFrame]:
    def _select_data(df):
        try:
            return Right(df.head(-1)[["tru_odds", "odds"]])
        except KeyError as e:
            return Left("Malformed odds table: %s" % e)

    @curry(2)
    def _add_colums(race_status, df):
        df = df.assign(datetime_retrieved=race_status["datetime_retrieved"])
        df = df.assign(mtp=race_status["mtp"])
        df = df.assign(wagering_closed=race_status["wagering_closed"])
        df = df.assign(results_posted=race_status["results_posted"])
        return Right(df)

    return (
        _get_table(soup, "amw_odds", {"id": "matrixTableOdds"})
        .bind(_select_data)
        .bind(_add_runner_id_by_tab(runners))
        .bind(_add_colums(race_status))
        .either(lambda x: Left("Cannot scrape odds: %s" % x), Right)
    )


def scrape_results(
    soup: BeautifulSoup, runners: Runner
) -> Either[str, pandas.DataFrame]:
    @curry(2)
    def _add_results(runners: Runner, results: pandas.DataFrame):
        results = results.to_dict("records")

        def _add(runner, result):
            runner.result = result["result"]
            return runner

        for result in results:
            runners = [
                _add(runner, result) if runner.tab == result["tab"] else runner
                for runner in runners
            ]
        return Right(runners)

    def _get_results(soup) -> Either[str, pandas.DataFrame]:
        if _results_visible(soup):
            attrs = {"class": "table table-Result table-Result-main"}
            return _get_table(soup, "amw_results", attrs)
        else:
            return Left("Results table not visible")

    return (
        _get_results(soup)
        .bind(_add_results(runners))
        .either(lambda x: Left("Cannot scrape results: %s" % x), Right)
    )


def scrape_individual_pools(
    race_status: dict,
    soup: BeautifulSoup,
    runners: list[Runner],
) -> Either[str, pandas.DataFrame]:
    def _select_data(df):
        try:
            return Right(df.head(-1)[["win_pool", "place_pool", "show_pool"]])
        except KeyError as e:
            return Left("Malformed odds table: %s" % e)

    @curry(2)
    def _add_colums(race_status, df):
        df = df.assign(datetime_retrieved=race_status["datetime_retrieved"])
        df = df.assign(mtp=race_status["mtp"])
        df = df.assign(wagering_closed=race_status["wagering_closed"])
        df = df.assign(results_posted=race_status["results_posted"])
        return Right(df)

    return (
        _get_table(soup, "amw_odds", {"id": "matrixTableOdds"})
        .bind(_select_data)
        .bind(_add_runner_id_by_tab(runners))
        .bind(_add_colums(race_status))
        .either(lambda x: Left("Cannot scrape individual pools: %s" % x), Right)
    )


def scrape_exotic_totals(
    soup: BeautifulSoup, race_id: int, platform_id: int, race_status: dict[str, object]
) -> Either[str, pandas.DataFrame]:
    def _append_multi_race(single_race) -> Either[str, pandas.DataFrame]:
        return _get_table(
            soup, "amw_multi_race_exotic_totals", {"id": "totalsRace"}
        ).either(
            lambda x: Left("Could not get multi race exotic totals: %s" % x),
            lambda x: Right(single_race.append(x, ignore_index=True)),
        )

    def _map_bet_types(df: pandas.DataFrame):
        df["bet_type"] = df["bet_type"].str.replace(r" (.+)", "", regex=True)
        mappings = resources.get_bet_type_mappings()
        if not df["bet_type"].isin(mappings).all():
            return Left("Unknown bet type in column: %s" % df["bet_type"].to_list())
        df["bet_type"] = df[["bet_type"]].replace(mappings)
        return Right(df)

    @curry(4)
    def _assign_columns(
        race_status: dict[str, object],
        race_id: int,
        platform_id: int,
        df: pandas.DataFrame,
    ):
        df = df.assign(race_id=race_id)
        df = df.assign(platform_id=platform_id)
        df = df.assign(datetime_retrieved=race_status["datetime_retrieved"])
        df = df.assign(mtp=race_status["mtp"])
        df = df.assign(wagering_closed=race_status["wagering_closed"])
        df = df.assign(results_posted=race_status["results_posted"])
        return Right(df)

    return (
        _get_table(soup, "amw_multi_leg_exotic_totals", {"id": "totalsLegs"})
        .bind(_append_multi_race)
        .bind(_map_bet_types)
        .bind(_assign_columns(race_status, race_id, platform_id))
        .either(lambda x: Left("Cannot scrape exotic totals: %s" % x), Right)
    )
