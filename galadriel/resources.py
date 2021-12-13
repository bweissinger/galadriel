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
        "amw_payout": {"class": "table table-Result table-Result-Pool"},
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
        "amw_payout": "table",
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
        "amw_payout": {
            "Pool Name": "bet_type",
            "Finish": "winners",
            "Wager": "wager",
            "Payout": "payout",
            "Total Pool": "total",
        },
    }
    mappings["amw_exacta_odds"] = mappings["amw_double_odds"]
    mappings["amw_quinella_odds"] = mappings["amw_double_odds"]
    return mappings[alias]


def get_individual_bet_type_mappings() -> Dict[str, str]:
    mappings = {"WIN": "win", "PLC": "place", "SHW": "show"}
    return mappings


def get_bet_type_mappings() -> Dict[str, str]:
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


def get_full_name_exotic_bet_mappings() -> Dict[str, str]:
    mappings = {
        "EXACTA": "exacta",
        "QUINELLA": "quinella",
        "TRIFECTA": "trifecta",
        "SUPERFECTA": "superfecta",
        "DOUBLE": "double",
        "PICK 3": "pick_3",
        "PICK 4": "pick_4",
        "PICK 5": "pick_5",
        "PICK 6": "pick_6",
    }
    return mappings


def get_table_converters(alias) -> Dict[str, object]:
    converters = {
        "amw_runners": {1: str},
        "amw_odds": {
            "WIN $": str,
            "PLC $": str,
            "SHW $": str,
            "TRU Odds": str,
            "WIN Odds": str,
        },
        "amw_results": {},
        "amw_multi_race_exotic_totals": {"Multi Race": str, "Multi Race.1": str},
        "amw_multi_leg_exotic_totals": {"Multi Race": str, "Multi Race.1": str},
        "amw_individual_totals": {},
        "amw_willpays": {},
        "amw_payout": {"Wager": str, "Payout": str},
        "rns_stats": {
            "Horse": str,
            "Tab": int,
            "Form L3": str,  # Needs Parsed
            "Form L5": str,  # Needs Parsed
            "Jockey": str,
            "Trainer": str,
            "Wgt": float,
            "BP": int,
            "BP Adj": int,
            "A": int,
            "S": str,  # Enum?
            "Car Best": float,
            "Sea Best": float,
            "JRat": float,
            "TRat": float,
            "RTC": str,
            "DLW": str,
            "RLW": int,
            "DLR": int,
            "WTC": float,
            "DC": int,
            "PM Car": float,
            "PM 12m": float,
            "EST": float,
            "BRR": float,
            "BR12": float,
            "Rat GF": float,
            "Rat SH": float,
            "LSRat": str,  # Requires parsing
            "50D Rat": float,
            "BL3": float,
            "API": float,
            "PP": float,
            "HiWT": float,
            "DOD": float,
            "JH": str,
            "JH%": str,
            "Car": str,
            "Car%": str,
            "12m": str,
            "12m%": str,
            "Crs": str,
            "Crs%": str,
            "CD": str,
            "CD%": str,
            "Dist": str,
            "Dist%": str,
            "F": str,
            "F%": str,
            "GD": str,
            "GD%": str,
            "SH": str,
            "SH%": str,
            "AW": str,
            "AW%": str,
            "Turf": str,
            "Turf%": str,
            "G1": str,
            "G1%": str,
            "G2": str,
            "G2%": str,
            "G3": str,
            "G3%": str,
            "LR": str,
            "LR%": str,
            "FU": str,
            "FU%": str,
            "2U": str,
            "2U%": str,
            "3U": str,
            "3U%": str,
            "CW": str,
            "CW%": str,
            "ACW": str,
            "ACW%": str,
            "FR": float,
            "Em": float,
            "Div": float,
            "P": str,  # Needs parsing
            "AES": float,
            "AFS": float,
            "NR": float,
            "PR": float,
        },
    }
    return converters[alias]


def get_rns_scraper_url_data() -> Dict[str, object]:
    return {
        "prefix": "https://www.racingandsports.com/form-guide/GenerateRaceGuide?discipline={}&country={}&course={}&cols=",
        "queries": [
            '[{"name"%3A"HTab"%2C"title"%3A"Tab"%2C"type"%3A"ND"%2C"size"%3A"0.75"}%2C{"name"%3A"HName"%2C"title"%3A"Horse"%2C"type"%3A"S"%2C"size"%3A"2.25"}%2C{"name"%3A"FormFigs3"%2C"title"%3A"Form L3"%2C"type"%3A"ND"%2C"size"%3A"0.75"}%2C{"name"%3A"FormFigs5"%2C"title"%3A"Form L5"%2C"type"%3A"ND"%2C"size"%3A"0.9375"}%2C{"name"%3A"Jockey"%2C"title"%3A"Jockey"%2C"type"%3A"S"%2C"size"%3A"2.25"}%2C{"name"%3A"Trainer"%2C"title"%3A"Trainer"%2C"type"%3A"S"%2C"size"%3A"2.25"}%2C{"name"%3A"HWeight"%2C"title"%3A"Wgt"%2C"type"%3A"D"%2C"size"%3A"0.75"}%2C{"name"%3A"HBP"%2C"title"%3A"BP"%2C"type"%3A"ND"%2C"size"%3A"0.75"}%2C{"name"%3A"HBPAdj"%2C"title"%3A"BP Adj"%2C"type"%3A"ND"%2C"size"%3A"0.75"}%2C{"name"%3A"HAge"%2C"title"%3A"A"%2C"type"%3A"ND"%2C"size"%3A"0.375"}%2C{"name"%3A"HSex"%2C"title"%3A"S"%2C"type"%3A"S"%2C"size"%3A"0.375"}%2C{"name"%3A"CarBest"%2C"title"%3A"Car Best"%2C"type"%3A"D"%2C"size"%3A"0.75"}%2C{"name"%3A"SeaBest"%2C"title"%3A"Sea Best"%2C"type"%3A"D"%2C"size"%3A"0.75"}%2C{"name"%3A"JRat"%2C"title"%3A"JRat"%2C"type"%3A"D"%2C"size"%3A"0.5625"}%2C{"name"%3A"TRat"%2C"title"%3A"TRat"%2C"type"%3A"D"%2C"size"%3A"0.5625"}%2C{"name"%3A"RFS"%2C"title"%3A"RTC"%2C"type"%3A"S"%2C"size"%3A"0.5625"}%2C{"name"%3A"DLW"%2C"title"%3A"DLW"%2C"type"%3A"ND"%2C"size"%3A"0.75"}%2C{"name"%3A"RLW"%2C"title"%3A"RLW"%2C"type"%3A"ND"%2C"size"%3A"0.75"}%2C{"name"%3A"DLR"%2C"title"%3A"DLR"%2C"type"%3A"ND"%2C"size"%3A"0.75"}%2C{"name"%3A"WTD"%2C"title"%3A"WTC"%2C"type"%3A"D"%2C"size"%3A"0.75"}%2C{"name"%3A"DTD"%2C"title"%3A"DC"%2C"type"%3A"ND"%2C"size"%3A"0.75"}%2C{"name"%3A"PMCar"%2C"title"%3A"PM Car"%2C"type"%3A"P"%2C"size"%3A"1.5"}%2C{"name"%3A"PM12m"%2C"title"%3A"PM 12m"%2C"type"%3A"P"%2C"size"%3A"1.5"}%2C{"name"%3A"PredRat"%2C"title"%3A"EST"%2C"type"%3A"D"%2C"size"%3A"0.75"}%2C{"name"%3A"Brr"%2C"title"%3A"BRR"%2C"type"%3A"D"%2C"size"%3A"0.75"}%2C{"name"%3A"BestRat12m"%2C"title"%3A"BR12"%2C"type"%3A"D"%2C"size"%3A"0.75"}%2C{"name"%3A"RatGF"%2C"title"%3A"Rat GF"%2C"type"%3A"D"%2C"size"%3A"0.75"}%2C{"name"%3A"RatSH"%2C"title"%3A"Rat SH"%2C"type"%3A"D"%2C"size"%3A"0.75"}%2C{"name"%3A"LSRat"%2C"title"%3A"LSRat"%2C"type"%3A"D"%2C"size"%3A"0.75"}]&addCols=["prizemoney"]&fs=S&page=land&preview=true',
            '[{"name"%3A"HTab"%2C"title"%3A"Tab"%2C"type"%3A"ND"%2C"size"%3A"0.75"}%2C{"name"%3A"HName"%2C"title"%3A"Horse"%2C"type"%3A"S"%2C"size"%3A"2.25"}%2C{"name"%3A"LSDet"%2C"title"%3A"LS Det"%2C"type"%3A"S"%2C"size"%3A"2.25"}%2C{"name"%3A"R50dRat"%2C"title"%3A"50D Rat"%2C"type"%3A"D"%2C"size"%3A"1.125"}%2C{"name"%3A"BL3"%2C"title"%3A"BL3"%2C"type"%3A"D"%2C"size"%3A"1.125"}%2C{"name"%3A"API"%2C"title"%3A"API"%2C"type"%3A"D"%2C"size"%3A"0.75"}%2C{"name"%3A"PPMark"%2C"title"%3A"PP"%2C"type"%3A"D"%2C"size"%3A"0.75"}%2C{"name"%3A"MaxWWT"%2C"title"%3A"HiWT"%2C"type"%3A"D"%2C"size"%3A"0.75"}%2C{"name"%3A"DOD"%2C"title"%3A"DOD"%2C"type"%3A"D"%2C"size"%3A"0.75"}%2C{"name"%3A"WPSJH"%2C"title"%3A"JH"%2C"type"%3A"D"%2C"size"%3A"1.125"}%2C{"name"%3A"WPSPerJH"%2C"title"%3A"JH%25"%2C"type"%3A"D"%2C"size"%3A"1.125"}%2C{"name"%3A"WPSCar"%2C"title"%3A"Car"%2C"type"%3A"D"%2C"size"%3A"1.125"}%2C{"name"%3A"WPSPerCar"%2C"title"%3A"Car%25"%2C"type"%3A"D"%2C"size"%3A"1.125"}%2C{"name"%3A"WPS12m"%2C"title"%3A"12m"%2C"type"%3A"D"%2C"size"%3A"1.125"}%2C{"name"%3A"WPSPer12m"%2C"title"%3A"12m%25"%2C"type"%3A"D"%2C"size"%3A"1.125"}%2C{"name"%3A"WPSCrs"%2C"title"%3A"Crs"%2C"type"%3A"D"%2C"size"%3A"1.125"}%2C{"name"%3A"WPSPerCrs"%2C"title"%3A"Crs%25"%2C"type"%3A"D"%2C"size"%3A"1.125"}%2C{"name"%3A"WPSCrsDist"%2C"title"%3A"CD"%2C"type"%3A"D"%2C"size"%3A"1.125"}%2C{"name"%3A"WPSPerCrsDist"%2C"title"%3A"CD%25"%2C"type"%3A"D"%2C"size"%3A"1.125"}%2C{"name"%3A"WPSDist"%2C"title"%3A"Dist"%2C"type"%3A"D"%2C"size"%3A"1.125"}%2C{"name"%3A"WPSPerDist"%2C"title"%3A"Dist%25"%2C"type"%3A"D"%2C"size"%3A"1.125"}%2C{"name"%3A"WPSF"%2C"title"%3A"F"%2C"type"%3A"D"%2C"size"%3A"1.125"}%2C{"name"%3A"WPSPerF"%2C"title"%3A"F%25"%2C"type"%3A"D"%2C"size"%3A"1.125"}%2C{"name"%3A"WPSGD"%2C"title"%3A"GD"%2C"type"%3A"D"%2C"size"%3A"1.125"}%2C{"name"%3A"WPSPerGD"%2C"title"%3A"GD%25"%2C"type"%3A"D"%2C"size"%3A"1.125"}]&addCols=["prizemoney"]&fs=S&page=land&preview=true',
            '[{"name"%3A"HTab"%2C"title"%3A"Tab"%2C"type"%3A"ND"%2C"size"%3A"0.75"}%2C{"name"%3A"HName"%2C"title"%3A"Horse"%2C"type"%3A"S"%2C"size"%3A"2.25"}%2C{"name"%3A"WPSSH"%2C"title"%3A"SH"%2C"type"%3A"D"%2C"size"%3A"1.125"}%2C{"name"%3A"WPSPerSH"%2C"title"%3A"SH%25"%2C"type"%3A"D"%2C"size"%3A"1.125"}%2C{"name"%3A"WPSAW"%2C"title"%3A"AW"%2C"type"%3A"D"%2C"size"%3A"1.125"}%2C{"name"%3A"WPSPerAW"%2C"title"%3A"AW%25"%2C"type"%3A"D"%2C"size"%3A"1.125"}%2C{"name"%3A"WPSTur"%2C"title"%3A"Turf"%2C"type"%3A"D"%2C"size"%3A"1.125"}%2C{"name"%3A"WPSPerTur"%2C"title"%3A"Turf%25"%2C"type"%3A"D"%2C"size"%3A"1.125"}%2C{"name"%3A"WPSG1"%2C"title"%3A"G1"%2C"type"%3A"D"%2C"size"%3A"1.125"}%2C{"name"%3A"WPSPerG1"%2C"title"%3A"G1%25"%2C"type"%3A"D"%2C"size"%3A"1.125"}%2C{"name"%3A"WPSG2"%2C"title"%3A"G2"%2C"type"%3A"D"%2C"size"%3A"1.125"}%2C{"name"%3A"WPSPerG2"%2C"title"%3A"G2%25"%2C"type"%3A"D"%2C"size"%3A"1.125"}%2C{"name"%3A"WPSG3"%2C"title"%3A"G3"%2C"type"%3A"D"%2C"size"%3A"1.125"}%2C{"name"%3A"WPSPerG3"%2C"title"%3A"G3%25"%2C"type"%3A"D"%2C"size"%3A"1.125"}%2C{"name"%3A"WPSLR"%2C"title"%3A"LR"%2C"type"%3A"D"%2C"size"%3A"1.125"}%2C{"name"%3A"WPSPerLR"%2C"title"%3A"LR%25"%2C"type"%3A"D"%2C"size"%3A"1.125"}%2C{"name"%3A"WPSFU"%2C"title"%3A"FU"%2C"type"%3A"D"%2C"size"%3A"1.125"}%2C{"name"%3A"WPSPerFU"%2C"title"%3A"FU%25"%2C"type"%3A"D"%2C"size"%3A"1.125"}%2C{"name"%3A"WPS2U"%2C"title"%3A"2U"%2C"type"%3A"D"%2C"size"%3A"1.125"}%2C{"name"%3A"WPSPer2U"%2C"title"%3A"2U%25"%2C"type"%3A"D"%2C"size"%3A"1.125"}%2C{"name"%3A"WPS3U"%2C"title"%3A"3U"%2C"type"%3A"D"%2C"size"%3A"1.125"}%2C{"name"%3A"WPSPer3U"%2C"title"%3A"3U%25"%2C"type"%3A"N"%2C"size"%3A"1.125"}%2C{"name"%3A"WPSCW"%2C"title"%3A"CW"%2C"type"%3A"N"%2C"size"%3A"1.125"}%2C{"name"%3A"WPSPerCW"%2C"title"%3A"CW%25"%2C"type"%3A"N"%2C"size"%3A"1.125"}]&addCols=["prizemoney"]&fs=S&page=land&preview=true',
            '[{"name"%3A"HTab"%2C"title"%3A"Tab"%2C"type"%3A"ND"%2C"size"%3A"0.75"}%2C{"name"%3A"HName"%2C"title"%3A"Horse"%2C"type"%3A"S"%2C"size"%3A"2.25"}%2C{"name"%3A"WPSACW"%2C"title"%3A"ACW"%2C"type"%3A"N"%2C"size"%3A"1.125"}%2C{"name"%3A"WPSPerACW"%2C"title"%3A"ACW%25"%2C"type"%3A"N"%2C"size"%3A"1.125"}%2C{"name"%3A"Fr"%2C"title"%3A"FR"%2C"type"%3A"D"%2C"size"%3A"0.75"}%2C{"name"%3A"Em"%2C"title"%3A"Em"%2C"type"%3A"D"%2C"size"%3A"0.75"}%2C{"name"%3A"Div"%2C"title"%3A"Div"%2C"type"%3A"D"%2C"size"%3A"0.75"}%2C{"name"%3A"Pace"%2C"title"%3A"P"%2C"type"%3A"S"%2C"size"%3A"0.75"}%2C{"name"%3A"AES"%2C"title"%3A"AES"%2C"type"%3A"D"%2C"size"%3A"0.75"}%2C{"name"%3A"AFS"%2C"title"%3A"AFS"%2C"type"%3A"D"%2C"size"%3A"0.75"}%2C{"name"%3A"NRat"%2C"title"%3A"NR"%2C"type"%3A"D"%2C"size"%3A"0.75"}%2C{"name"%3A"PR"%2C"title"%3A"PR"%2C"type"%3A"D"%2C"size"%3A"0.75"}]&addCols=["prizemoney"]&fs=S&page=land&preview=true',
        ],
    }


def get_rns_columns_map() -> Dict[str, str]:
    return {
        "Form L3": "form_3_starts",
        "Form L5": "form_5_starts",
        "Jockey": "jockey",
        "Trainer": "trainer",
        "Wgt": "weight",
        "BP": "barrier_position",
        "BP Adj": "barrier_position_adjusted",
        "A": "age",
        "S": "sex",
        "Car Best": "career_best",
        "Sea Best": "season_best",
        "JRat": "jockey_rating",
        "TRat": "trainer_rating",
        "RTC": "runs_this_campaign",
        "DLW": "days_since_last_win",
        "RLW": "runs_since_last_win",
        "DLR": "days_since_last_run",
        "WTC": "weight_change",
        "DC": "distance_change",
        "PM Car": "average_prize_money_career",
        "PM 12m": "average_prize_money_12_months",
        "EST": "predicted_rating",
        "BRR": "base_run_rating",
        "BR12": "best_rating_12_months",
        "Rat GF": "rating_good_to_fast",
        "Rat SH": "rating_soft_to_heavy",
        "LSRat": "last_start_rating",
        "LS Det": "last_start_details",
        "50D Rat": "ratings_50_days",
        "BL3": "best_rating_last_3_runs",
        "API": "api",
        "PP": "prepost_markets",
        "HiWT": "highest_winning_weight",
        "DOD": "degree_of_difficulty",
        "JH": "wps_jockey_and_horse",
        "JH%": "wps_percent_jockey_and_horse",
        "Car": "wps_career",
        "Car%": "wps_percent_career",
        "12m": "wps_12_month",
        "12m%": "wps_percent_12_month",
        "Crs": "wps_course",
        "Crs%": "wps_percent_course",
        "CD": "wps_course_and_distance",
        "CD%": "wps_percent_course_and_distance",
        "Dist": "wps_distance",
        "Dist%": "wps_percent_distance",
        "F": "wps_fast",
        "F%": "wps_percent_fast",
        "GD": "wps_good_to_dead",
        "GD%": "wps_percent_good_to_dead",
        "SH": "wps_soft_to_heavy",
        "SH%": "wps_percent_soft_to_heavy",
        "AW": "wps_all_weather",
        "AW%": "wps_percent_all_weather",
        "Turf": "wps_turf",
        "Turf%": "wps_percent_turf",
        "G1": "wps_group_1",
        "G1%": "wps_percent_group_1",
        "G2": "wps_group_2",
        "G2%": "wps_percent_group_2",
        "G3": "wps_group_3",
        "G3%": "wps_percent_group_3",
        "LR": "wps_listed_race",
        "LR%": "wps_percent_listed_race",
        "FU": "wps_first_up",
        "FU%": "wps_percent_first_up",
        "2U": "wps_second_up",
        "2U%": "wps_percent_second_up",
        "3U": "wps_third_up",
        "3U%": "wps_percent_third_up",
        "CW": "wps_clockwise",
        "CW%": "wps_percent_clockwise",
        "ACW": "wps_anti_clockwise",
        "ACW%": "wps_percent_anti_clockwise",
        "FR": "final_rating",
        "Em": "theoretical_beaten_margin",
        "Div": "dividend",
        "P": "speed_map_pace",
        "AES": "early_speed_figure",
        "AFS": "final_speed_figure",
        "NR": "neural_algorithm_rating",
        "PR": "neural_algorithm_price",
    }
