#!/usr/bin/env python3
from scripts.helpers.league_records import predict_match_outcome


def answer(db):
    return predict_match_outcome(db=db, league_code='I1', home_team_name='Torino', away_team_name='Inter')
