import re
import pandas
import operator

from bs4 import BeautifulSoup
from datetime import datetime, timedelta
from sqlalchemy.sql.sqltypes import DateTime
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
    soup: BeautifulSoup,
    table_alias: str,
    map_names: bool = True,
) -> Either[str, pandas.DataFrame]:
    table_attrs = resources.get_table_attrs(table_alias)
    search_tag = resources.get_search_tag(table_alias)
    search = soup.find(search_tag, table_attrs)
    try:
        table = pandas.read_html(str(search))[0]
        if map_names:
            return _map_dataframe_table_names(table, table_alias)
        return Right(table)
    except ValueError as e:
        return Left("Unable to find table %s" % table_alias)


def _sort_runners(runners: list[Runner]) -> Either[str, list[Runner]]:
    try:
        return Right(sorted(runners, key=operator.attrgetter("tab")))
    except (AttributeError, TypeError) as e:
        return Left("Unable to add runner ids to DataFrame: %s" % e)


@curry(2)
def _add_runner_id_by_tab(
    runners: list[Runner], data_frame: pandas.DataFrame
) -> Either[str, pandas.DataFrame]:
    @curry(2)
    def _add_to_df(data_frame, runners):
        ids = [runner.id for runner in runners]
        try:
            data_frame = data_frame.assign(runner_id=ids)
            return Right(data_frame)
        except ValueError as e:
            return Left("Unable to add runner ids to DataFrame: %s" % e)

    return _sort_runners(runners).bind(_add_to_df(data_frame))


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

    runners = _get_table(soup, "amw_runners")

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
            return Left("Could not find post time element in page")

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
        tz = ZoneInfo(str(get_localzone()))
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

    return (
        Right({"datetime_retrieved": datetime_retrieved})
        .bind(_add_mtp)
        .bind(_add_results)
        .bind(_add_wagering)
    )


def get_track_list(soup: BeautifulSoup) -> Either[str, list[dict[str, str]]]:
    def _find_track_list(soup):
        search_re = re.compile("event_selector event-status*")
        races = soup.find_all("a", {"class": search_re})
        if len(races) == 0:
            return Left("Could not find track list in page.")
        return Right(races)

    def _create_dicts(race_list):
        try:
            return Right([{"id": race["id"], "html": str(race)} for race in race_list])
        except KeyError:
            return Left("Unknown formatting in race list.")

    return _find_track_list(soup).bind(_create_dicts)


def get_num_races(soup: BeautifulSoup) -> Either[str, int]:
    try:
        search = soup.find_all("button", {"id": re.compile("race-*")})
        nums = [int(x.text.rstrip()) for x in search if x.text != "All"]
        return Right(max(nums))
    except ValueError as e:
        return Left("Could not find number of races for this track: %s" % e)


def get_focused_race_num(soup: BeautifulSoup) -> Either[str, int]:
    search = soup.find("button", {"class": re.compile(r"r*track-num-fucus")})
    try:
        return Right(int(search.text))
    except (AttributeError, ValueError) as e:
        return Left("Unknown race focus status: %s" % str(e))


def scrape_race(
    soup: BeautifulSoup, datetime_retrieved: datetime, meet_id: int
) -> Either[str, pandas.DataFrame]:
    def _add_race_num(df):
        race_num = get_focused_race_num(soup)
        return race_num.bind(lambda x: Right(df.assign(race_num=[x])))

    def _add_est_post(df):
        def _create_dict(mtp):
            return Right(
                {"estimated_post": [datetime_retrieved + timedelta(minutes=mtp)]}
            )

        mtp = get_mtp(soup, datetime_retrieved)
        return mtp.bind(_create_dict).bind(lambda x: Right(df.assign(**x)))

    def _add_discipline(df):
        return get_discipline(soup).bind(
            lambda x: Right(df.assign(**{"discipline_id": [x]}))
        )

    return (
        Right(
            pandas.DataFrame(
                {"meet_id": [meet_id], "datetime_retrieved": [datetime_retrieved]}
            )
        )
        .bind(_add_race_num)
        .bind(_add_est_post)
        .bind(_add_discipline)
        .either(lambda x: Left("Cannot scrape race: %s" % x), Right)
    )


def scrape_runners(soup: BeautifulSoup, race_id: int) -> Either[str, pandas.DataFrame]:
    def _select_columns(df):
        try:
            return Right(df[["name", "tab", "morning_line"]])
        except KeyError as e:
            return Left("Cannot select columns from runner table: %s" % e)

    return (
        _get_table(soup, "amw_runners")
        .bind(_select_columns)
        .bind(_assign_columns_from_dict({"race_id": race_id}))
        .either(lambda x: Left("Cannot scrape runners: %s" % x), Right)
    )


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

    return (
        _get_table(soup, "amw_odds")
        .bind(_select_data)
        .bind(_add_runner_id_by_tab(runners))
        .bind(_assign_columns_from_dict(race_status))
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
            return _get_table(soup, "amw_results")
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
    platform_id: int,
) -> Either[str, pandas.DataFrame]:
    def _select_data(df):
        try:
            return Right(df.head(-1)[["win", "place", "show"]])
        except KeyError as e:
            return Left("Malformed odds table: %s" % e)

    column_dict = race_status
    column_dict["platform_id"] = platform_id

    def print_stuff(table):
        print(table)
        return Right(table)

    return (
        _get_table(soup, "amw_odds")
        .bind(_select_data)
        .bind(_add_runner_id_by_tab(runners))
        .bind(_assign_columns_from_dict(column_dict))
        .either(lambda x: Left("Cannot scrape individual pools: %s" % x), Right)
    )


def scrape_exotic_totals(
    soup: BeautifulSoup, race_id: int, platform_id: int, race_status: dict[str, object]
) -> Either[str, pandas.DataFrame]:
    def _append_multi_race(single_race) -> Either[str, pandas.DataFrame]:
        return _get_table(soup, "amw_multi_race_exotic_totals").either(
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

    def _assign_columns(
        bets: pandas.DataFrame,
    ):
        def _construct_column(alias, bets):
            try:
                total = bets.loc[bets["bet_type"] == alias, "total"].to_list()[0]
                return {alias: total}
            except IndexError:
                return {alias: None}

        def _add_bet_types(df):
            columns = resources.get_bet_type_mappings().values()
            for column in columns:
                df = df.assign(**_construct_column(column, bets))
            return Right(df)

        df = pandas.DataFrame({"race_id": [race_id], "platform_id": platform_id})
        return _assign_columns_from_dict(race_status, df).bind(_add_bet_types)

    return (
        _get_table(soup, "amw_multi_leg_exotic_totals")
        .bind(_append_multi_race)
        .bind(_map_bet_types)
        .bind(_assign_columns)
        .either(lambda x: Left("Cannot scrape exotic totals: %s" % x), Right)
    )


def scrape_race_commissions(
    soup: BeautifulSoup, race_id: int, platform_id: int, datetime_retrieved: datetime
) -> Either[str, pandas.DataFrame]:
    def _append_multi_race(single_race) -> Either[str, pandas.DataFrame]:
        return _get_table(soup, "amw_multi_race_exotic_totals").either(
            lambda x: Left("Could not get multi race exotic totals: %s" % x),
            lambda x: Right(single_race.append(x, ignore_index=True)),
        )

    def _map_bet_types(df: pandas.DataFrame):
        split_string = df["bet_type"].str.split(" ", n=1, expand=True)
        df["bet_type"] = split_string[0]
        df["commission"] = (
            split_string[1]
            .str.replace("(", "", regex=False)
            .str.replace("%)", "", regex=False)
        )
        mappings = resources.get_bet_type_mappings()
        if not df["bet_type"].isin(mappings).all():
            return Left("Unknown bet type in column: %s" % df["bet_type"].to_list())
        df["bet_type"] = df[["bet_type"]].replace(mappings)
        return Right(df)

    @curry(4)
    def _assign_columns(
        datetime_retrieved: datetime_retrieved,
        race_id: int,
        platform_id: int,
        bets: pandas.DataFrame,
    ):
        def _construct_column(alias, bets):
            try:
                commission = bets.loc[
                    bets["bet_type"] == alias, "commission"
                ].to_list()[0]
                return {alias: commission}
            except IndexError:
                return {alias: None}

        df = pandas.DataFrame({"race_id": [race_id], "platform_id": [platform_id]})
        df = df.assign(datetime_retrieved=datetime_retrieved)

        columns = resources.get_bet_type_mappings().values()
        for column in columns:
            df = df.assign(**_construct_column(column, bets))

        return Right(df)

    def _add_individual_commissions(df: pandas.DataFrame):
        @curry(2)
        def _split_columns(
            df: pandas.DataFrame,
            individual_commissions: pandas.DataFrame,
        ):

            columns = individual_commissions.drop(columns=["Runner"]).columns
            mappings = resources.get_individual_bet_type_mappings()
            try:
                assigned = []
                for column in columns:
                    split_string = column.split(" ")
                    bet_type = mappings[split_string[0]]
                    commission = split_string[1].replace("(", "").replace("%)", "")
                    df = df.assign(**{bet_type: commission})
                    assigned.append(bet_type)

                for column in set(mappings.values()) - set(assigned):
                    df = df.assign(**{column: None})
                return Right(df)
            except KeyError as e:
                return Left("Unknown bet type: %s" % str(e))

        return (
            _get_table(soup, "amw_individual_totals", map_names=False)
            .bind(_split_columns(df))
            .either(lambda x: Left("Cannot add individual commissions: %s" % x), Right)
        )

    return (
        _get_table(soup, "amw_multi_leg_exotic_totals")
        .bind(_append_multi_race)
        .bind(_map_bet_types)
        .bind(_assign_columns(datetime_retrieved, race_id, platform_id))
        .bind(_add_individual_commissions)
        .either(lambda x: Left("Cannot scrape race commissions: %s" % x), Right)
    )


@curry(2)
def _assign_columns_from_dict(
    column_dict: dict[str, object], data_frame: pandas.DataFrame
):
    return Right(data_frame.assign(**column_dict))


def _scrape_two_runner_odds_table(
    soup: BeautifulSoup,
    runners_race_1: list[Runner],
    table_alias: str,
    race_status: dict,
    runners_race_2=None,
) -> Either[str, pandas.DataFrame]:
    def _prep_table(table):
        def _correct_id_offset(df):
            df.runner_1_id += 1
            return Right(df)

        try:
            table = table.drop(columns=["1/2"])
            table = table.stack().reset_index()
            return _map_dataframe_table_names(table, table_alias).bind(
                _correct_id_offset
            )
        except KeyError as e:
            return Left("Cannot stack data frame: %s" % str(e))

    def _substitute_id_for_tab(table):
        id_map_race_1 = {runner.tab: runner.id for runner in runners_race_1}
        if runners_race_2:
            id_map_race_2 = {runner.tab: runner.id for runner in runners_race_2}
        else:
            id_map_race_2 = id_map_race_1
        table.runner_1_id = table.runner_1_id.astype("int32")
        table.runner_2_id = table.runner_2_id.astype("int32")
        if set(table.runner_1_id) != set(id_map_race_1) or set(
            table.runner_2_id
        ) != set(id_map_race_2):
            return Left(
                "Cannot add runner id's: Runner tabs in table do not match supplied"
                " runners. supplied_race_1: %s, table_race_1: %s, "
                "supplied_race_2: %s, table_race_2: %s"
                % (
                    set(id_map_race_1),
                    set(table.runner_1_id),
                    set(id_map_race_2),
                    set(table.runner_2_id),
                )
            )
        table.runner_1_id = table.runner_1_id.replace(id_map_race_1)
        table.runner_2_id = table.runner_2_id.replace(id_map_race_2)
        return Right(table)

    return (
        _get_table(soup, table_alias, map_names=False)
        .bind(_prep_table)
        .bind(_substitute_id_for_tab)
        .bind(_assign_columns_from_dict(race_status))
    )


def scrape_double_odds(
    soup: BeautifulSoup,
    runners_race_1: list[Runner],
    runners_race_2: list[Runner],
    race_status: dict[str, object],
) -> Either[str, pandas.DataFrame]:
    return _scrape_two_runner_odds_table(
        soup,
        runners_race_1,
        "amw_double_odds",
        race_status,
        runners_race_2=runners_race_2,
    ).either(lambda x: Left("Cannot scrape double odds: %s" % x), Right)


def scrape_exacta_odds(
    soup: BeautifulSoup, runners: list[Runner], race_status: dict[str, object]
) -> Either[str, pandas.DataFrame]:
    return _scrape_two_runner_odds_table(
        soup, runners, "amw_exacta_odds", race_status
    ).either(lambda x: Left("Cannot scrape exacta odds: %s" % x), Right)


def scrape_quinella_odds(
    soup: BeautifulSoup, runners: list[Runner], race_status: dict[str, object]
) -> Either[str, pandas.DataFrame]:
    return _scrape_two_runner_odds_table(
        soup, runners, "amw_quinella_odds", race_status
    ).either(lambda x: Left("Cannot scrape quinella odds: %s" % x), Right)


def scrape_willpays(
    soup: BeautifulSoup,
    runners: list[Runner],
    platform_id: int,
    datetime_retrieved: DateTime,
) -> Either[str, pandas.DataFrame]:
    def _do_column_operations(data_frame):
        column_mappings = resources.get_bet_type_mappings()
        columns = data_frame.columns.to_list()
        for column in columns:
            bet_amount = column.split()[0].replace("$", "")
            data_frame[column] = data_frame[column].astype("float") / float(bet_amount)
            bet_type = column.split()[1]
            try:
                data_frame = data_frame.rename(
                    columns={column: column_mappings[bet_type]}
                )
            except KeyError as e:
                return Left("Error renaming column %s: %s" % (column, e))
        return Right(data_frame)

    def _drop_unnecesary_data(data_frame):
        try:
            # Remove results row
            data_frame = data_frame[data_frame["Unnamed: 0"] != "Results"]
            # Drop tab column
            return Right(data_frame.drop(columns=["Unnamed: 0"]).reset_index(drop=True))
        except KeyError as e:
            return Left("Could not drop data: %s" % e)

    additional_columns = {
        "datetime_retrieved": datetime_retrieved,
        "platform_id": platform_id,
    }
    return (
        _get_table(soup, "amw_willpays", map_names=False)
        .bind(_drop_unnecesary_data)
        .bind(_do_column_operations)
        .bind(_assign_columns_from_dict(additional_columns))
        .bind(_add_runner_id_by_tab(runners))
        .either(lambda x: Left("Cannot scrape willpays: %s" % x), Right)
    )


def scrape_payouts(
    soup: BeautifulSoup, race_id: int, platform_id: int, datetime_retrieved: DateTime
) -> Either[str, pandas.DataFrame]:
    def _parse_table(data_frame):
        # Bet types can have multiple sets of applicable runners
        # not sure what to do in this case, so skip parsing of
        # payout table
        if any(data_frame.bet_type.duplicated()):
            return Left("Multiples of same bet type found")

        # Remove dollar sign
        data_frame.payout = data_frame.payout.str.replace("$ ", "", regex=False)
        data_frame.wager = data_frame.wager.str.replace("$ ", "", regex=False)

        # Calculate payout per dollar
        data_frame.payout = data_frame.payout.astype("float") / data_frame.wager.astype(
            "float"
        )
        bet_types = resources.get_full_name_exotic_bet_mappings()

        # Transform table
        data_frame = (
            data_frame.pivot(columns="bet_type", values="payout").bfill().head(1)
        )
        data_frame = data_frame.rename_axis(None, axis=1)
        mask = data_frame.columns.intersection(bet_types.keys())
        data_frame = data_frame[mask].rename(columns=bet_types)
        return Right(data_frame)

    additional_columns = {
        "datetime_retrieved": datetime_retrieved,
        "platform_id": platform_id,
        "race_id": race_id,
    }

    return (
        _get_table(soup, "amw_payout")
        .bind(_parse_table)
        .bind(_assign_columns_from_dict(additional_columns))
        .either(lambda x: Left("Cannot scrape payout table: %s" % x), Right)
    )
