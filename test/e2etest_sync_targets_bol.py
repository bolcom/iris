#!/usr/bin/env python
#
# Inspect .ci/* to setup the tests and then run:
# make e2e_bol


import pytest
import json
import requests
import copy
import iris.bin.iris_ctl as iris_ctl
from click.testing import CliRunner
import uuid
import socket
import logging


from iris.bin.sync_targets_bol import get_teams_default_plans


def test_default_team_plans():
    scrumteam = 'team1a'
    itops_team = 'tes'

    standby_team = 'eod'
    standby_esc_team = 'mod'

    scrum_team_plans = [scrumteam + '-24x7-withsrt-builtin',
                        scrumteam + '-24x7-private-builtin',
                        scrumteam + '-workhours-private-builtin' ]
    standby_team_plans = [standby_team + '-24x7-private-builtin',
                          standby_team + '-24x7-withmod-builtin']
    itops_team_plans = [itops_team + '-24x7-private-builtin',
                        itops_team + '-workhours-private-builtin']

    platform_teams = [itops_team]
    standby_teams = [standby_team]
    standby_escalation_teams = [standby_esc_team]
    space_to_srt_mapping = {'retail': 'srt-retail'}
    teams_yaml = {
            scrumteam:
            {
                'space': 'retail',
                'motm_phase': '2',
                'mobile_phone': '316120394',
                'email_address': 'team1b@bol.com',
            }
    }

    plans = get_teams_default_plans(space_to_srt_mapping, scrumteam, platform_teams, standby_teams, standby_escalation_teams, teams_yaml)
    plan_names = [x['name'] for x in plans]
    assert sorted(scrum_team_plans) == sorted(plan_names)
    for plan in plans:
        if plan['name'] == 'team1b-24x7-private-builtin':
            assert plan['type'] == 'private-24x7'
            assert plan['extra_opts']['team'] == 'team1b-24x7-builtin'
        elif plan['name'] == 'team1b-workhours-builtin':
            assert plan['type'] == 'private-workhours'
            assert plan['extra_opts']['team'] == 'team1b-workhours-builtin'
        elif plan['name'] == 'team1b-24x7-withsrt-builtin':
            assert plan['type'] == 'scrumteam'
            assert plan['extra_opts']['team'] == 'team1b-workhours-builtin'
            assert plan['extra_opts']['escalation_team'] == 'srt-retail-builtin'
            assert plan['extra_opts']['standby_team'] == 'middleware-standby-builtin'
            assert plan['extra_opts']['standby_escalation_team'] == 'mod-standby-builtin'

    plans = get_teams_default_plans(space_to_srt_mapping, itops_team, platform_teams, standby_teams, standby_escalation_teams, teams_yaml)
    plan_names = [x['name'] for x in plans]
    assert sorted(itops_team_plans) == sorted(plan_names)
    for plan in plans:
        assert plan['name'] in itops_team_plans

    plans = get_teams_default_plans(space_to_srt_mapping, standby_team, platform_teams, standby_teams, standby_escalation_teams, teams_yaml)
    plan_names = [x['name'] for x in plans]
    assert sorted(standby_team_plans) == sorted(plan_names)
    for plan in plans:
        assert plan['name'] in standby_team_plans
