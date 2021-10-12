from typing import Dict


def get_table_attrs(alias: str) -> Dict[str, str]:
    mappings = {
        "amw_runners": {"id": "runner-view-inner-table"},
        "amw_odds": {"id": "matrixTableOdds"},
        "amw_results": {"class": "table table-Result table-Result-main"},
        "amw_multi_race_exotic_totals": {"id": "totalsRace"},
        "amw_multi_leg_exotic_totals": {"id": "totalsLegs"},
        "amw_individual_totals": {"id": "totalsRunner"},
        "amw_double_odds": {"id": "DBL-Matrix"},
        "amw_exacta_odds": {"id": "EX-Matrix"},
        "amw_quinella_odds": {"id": "QU-Matrix"},
        "amw_willpays": {"id": "matrixTableWillpays"},
    }
    return mappings[alias]


def get_search_tag(alias: str):
    tags = {
        "amw_runners": "table",
        "amw_odds": "table",
        "amw_results": "table",
        "amw_multi_race_exotic_totals": "table",
        "amw_multi_leg_exotic_totals": "table",
        "amw_individual_totals": "table",
        "amw_double_odds": "div",
        "amw_willpays": "table",
    }
    tags["amw_exacta_odds"] = tags["amw_double_odds"]
    tags["amw_quinella_odds"] = tags["amw_double_odds"]
    return tags[alias]


def get_table_map(alias: str) -> Dict[str, str]:
    mappings = {
        "amw_runners": {
            0: "name",
            1: "morning_line",
            2: "odds",
            3: "tab",
            4: "first_pick",
            5: "one_dollar_payout",
            6: "stake",
            7: "payout",
        },
        "amw_odds": {
            "Unnamed: 0": "tab",
            "TRU Odds": "tru_odds",
            "WIN Odds": "odds",
            "WIN $": "win",
            "%": "win_percent",
            "PLC $": "place",
            "%.1": "place_percent",
            "SHW $": "show",
            "%.2": "show_percent",
        },
        "amw_results": {
            "Pos.": "result",
            "Runner": "name",
            "Unnamed: 2": "tab",
            "Win": "win_payout",
            "Place": "place_payout",
            "Show": "Show_payout",
        },
        "amw_multi_leg_exotic_totals": {
            "Multi Leg": "bet_type",
            "Multi Leg.1": "total",
        },
        "amw_multi_race_exotic_totals": {
            "Multi Race": "bet_type",
            "Multi Race.1": "total",
        },
        "amw_double_odds": {
            "level_0": "runner_1_id",
            "level_1": "runner_2_id",
            0: "odds",
        },
    }
    mappings["amw_exacta_odds"] = mappings["amw_double_odds"]
    mappings["amw_quinella_odds"] = mappings["amw_double_odds"]
    return mappings[alias]


def get_individual_bet_type_mappings() -> dict[str, str]:
    mappings = {"WIN": "win", "PLC": "place", "SHW": "show"}
    return mappings


def get_bet_type_mappings() -> dict[str, str]:
    mappings = {
        "EX": "exacta",
        "QU": "quinella",
        "TRI": "trifecta",
        "SPR": "superfecta",
        "DBL": "double",
        "PK3": "pick_3",
        "PK4": "pick_4",
        "PK5": "pick_5",
        "PK6": "pick_6",
    }
    return mappings
