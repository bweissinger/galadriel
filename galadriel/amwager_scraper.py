import re
import pandas
import pytz
import operator

from bs4 import BeautifulSoup
from datetime import datetime, timedelta
from tzlocal import get_localzone
from pymonad.either import Left, Right, Either
from pymonad.tools import curry

from galadriel import resources
from galadriel.database import Runner


def _map_dataframe_table_names(
    df: pandas.DataFrame, alias: str
) -> Either[str, pandas.DataFrame]:

    try:
        df = df.rename(errors="raise", columns=resources.get_table_map(alias))
        return Right(df)
    except KeyError:
        return Left("Unable to map names.")


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
    except TypeError as e:
        return Left("Unable to add runners: %s" % e)

    ids = [runner.id for runner in runners]

    try:
        data_frame = data_frame.assign(runner_id=ids)
        return Right(data_frame)
    except ValueError:
        return Left("Mismatched runners and table length")


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

    div = soup.find("div", {"data-translate-lang": "wager.raceclosedmessage"})
    try:
        if div["style"] == "display: none;":
            return Right(False)
        elif div["style"] == "":
            return Right(True)
    except KeyError:
        return Left("Cannot deterimine wagering status.")


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
            post = datetime.strptime(text, "%I:%M %p")
        except ValueError:
            try:
                post = datetime.strptime(text, "%H:%M")
            except ValueError:
                return Left("Unknown time format: %s" % text)
        return Right(post.replace(tzinfo=get_localzone()).astimezone(pytz.UTC).time())

    def _post_time_to_mtp(post):
        est_post = datetime.combine(datetime_retrieved.date(), post, pytz.UTC)
        if datetime_retrieved >= est_post:
            est_post += timedelta(days=1)
        return Right(int((est_post - datetime_retrieved).total_seconds() / 60))

    def _convert_to_mtp(text):
        return _get_post_time(text).bind(_post_time_to_mtp)

    return _get_mtp_text(soup).bind(_get_int).either(_convert_to_mtp, Right)


def get_race_status(
    soup: BeautifulSoup, datetime_retrieved: datetime
) -> Either[str, dict[str, object]]:
    def _add(func, key, params, m_dict):
        tmp = func(*params)
        return tmp.either(
            lambda x: Left("Cannot obtain race status" + x),
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
        return Left("Could not find the race numbers for this race. " + str(e))


def get_focused_race_num(soup: BeautifulSoup) -> Either[str, int]:
    search = soup.find("button", {"class": re.compile(r"r*track-num-fucus")})
    try:
        return Right(int(search.text))
    except (AttributeError, ValueError) as e:
        return Left("Unknown race focus status." + str(e))


def scrape_race(
    soup: BeautifulSoup, datetime_retrieved: datetime, meet_id: int
) -> Either[str, pandas.DataFrame]:
    @curry(3)
    def _add_est_post(soup, datetime_retrieved, df):
        mtp = get_mtp(soup, datetime_retrieved)
        return mtp.bind(
            lambda x: Right({"mtp": [datetime_retrieved + timedelta(minutes=x)]})
        ).bind(lambda x: Right(df.assign(**x)))

    @curry(2)
    def _add_race_num(soup, df):
        race_num = get_focused_race_num(soup)
        return race_num.bind(lambda x: Right(df.assign(race_num=[x])))

    @curry(2)
    def _add_dt_retrieved(datetime_retrieved, df):
        return Right(df.assign(datetime_retrieved=[datetime_retrieved]))

    @curry(2)
    def _add_meed_id(meet_id, df):
        return Right(df.assign(**{"meet_id": [meet_id]}))

    return (
        Right(pandas.DataFrame())
        .bind(_add_race_num(soup))
        .bind(_add_est_post(soup, datetime_retrieved))
        .bind(_add_dt_retrieved(datetime_retrieved))
        .bind(_add_meed_id(meet_id))
    )


def scrape_runners(soup: BeautifulSoup, race_id: int) -> Either[str, pandas.DataFrame]:
    def _transform_table(df):
        df = df[["name", "tab", "morning_line"]]
        df = df.assign(race_id=race_id)
        return Right(df)

    runners_table = _get_table(soup, "amw_runners", {"id": "runner-view-inner-table"})

    return runners_table.bind(_transform_table)


def scrape_odds(
    race_status: dict,
    soup: BeautifulSoup,
    runners: list[Runner],
) -> Either[str, pandas.DataFrame]:
    def _select_data(df):
        try:
            return Right(df.head(-1)[["tru_odds", "odds"]])
        except KeyError:
            return Left("Malformed odds table.")

    @curry(2)
    def _add_colums(race_status, df):
        df = df.assign(datetime_retrieved=race_status["datetime_retrieved"])
        df = df.assign(mtp=race_status["mtp"])
        df = df.assign(wagering_closed=race_status["wagering_closed"])
        df = df.assign(results_posted=race_status["results_posted"])
        return Right(df)

    odds_table = _get_table(soup, "amw_odds", {"id": "matrixTableOdds"})
    return (
        odds_table.bind(_select_data)
        .bind(_add_runner_id_by_tab(runners))
        .bind(_add_colums(race_status))
    )
