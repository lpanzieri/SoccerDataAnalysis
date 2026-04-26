#!/usr/bin/env python3
from scripts.helpers.league_records import get_most_points_in_season


def answer(db):
    return get_most_points_in_season(db=db, league_code='I1', points_for_win=3, points_for_draw=1)
