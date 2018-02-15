# NOTE:
# This code has tests.
# Please use to test changes.
# Inspect .ci/* to setup the tests and then run:
# make e2e_bol

import datetime
from falcon import HTTPBadRequest, HTTPForbidden, HTTPUnauthorized, HTTPNotFound

from gevent import monkey, sleep, spawn
monkey.patch_all()  # NOQA

import logging
import os
from contextlib import contextmanager
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError, IntegrityError, DataError
import requests
from phonenumbers import format_number, parse, PhoneNumberFormat
from phonenumbers.phonenumberutil import NumberParseException

import ujson
import yaml
import sys
import copy

from iris.api import load_config
from iris import metrics


logging.getLogger('requests').setLevel(logging.WARNING)

NOOP = False

# used to detect scrumteams vs itops teams
scrumteam_prefix = 'team'

# all created objects are suffixed with:
immutable_suffix = '-builtin'

# used for plan naming
srt_suffix = '-withsrt'
mod_suffix = '-withmod'
private_suffix = '-private'

# used to point to teams that are used by plans
middleware_team = 'middleware'
mod_team = 'mod'

timeperiods = ['workhours', 'standby', '24x7', 'businesshours']

# plan defaults
default_template = "default"
default_wait = 1800
default_repeat = 0  # $repeat * $wait = escal time
default_escalation_repeat = 1
default_escalation_wait = 900
default_priority = "high"
default_standby_escalation_priority = "urgent"

default_plan_template = {
    'creator': 'dummy',
    'name': None,
    'created': None,
    'description': None,
    'step_count': None,
    'threshold_window': 900,
    'threshold_count': 10,
    'aggregation_window': 300,
    'aggregation_reset': 300,
    'tracking_key': None,
    'tracking_type': None,
    'tracking_template': None
}

default_scrumteam_plan_steps = [
    [
        {
            "priority": default_priority,
            "repeat": default_repeat,
            "role": "oncall-primary-exclude-holidays",
            "target": None,
            "template": default_template,
            "wait": default_wait,
        },
        {
            "priority": default_priority,
            "repeat": default_repeat,
            "role": "oncall-primary",
            "target": None,
            "template": default_template,
            "wait": default_wait,
        }
    ],
    [
        {
            "priority": default_priority,
            "repeat": default_escalation_repeat,
            "role": "oncall-primary-exclude-holidays",
            "target": None,
            "template": default_template,
            "wait": default_escalation_wait,
        },
        {
            "priority": default_standby_escalation_priority,
            "repeat": default_escalation_repeat,
            "role": "oncall-primary",
            "target": None,
            "template": default_template,
            "wait": default_escalation_wait,
        }
    ],
    [
        {
            "priority": default_standby_escalation_priority,
            "repeat": default_escalation_repeat,
            "role": "oncall-primary",
            "target": None,
            "template": default_template,
            "wait": default_escalation_wait,
        }
    ]
]

default_standby_plan_steps = [
    [
        {
            "priority": default_priority,
            "repeat": default_repeat,
            "role": "oncall-primary",
            "target": None,
            "template": default_template,
            "wait": default_wait,
        }
    ],
    [
        {
            "priority": default_standby_escalation_priority,
            "repeat": default_escalation_repeat,
            "role": "oncall-primary",
            "target": None,
            "template": default_template,
            "wait": default_escalation_wait,
        }
    ],
    [
        {
            "priority": default_standby_escalation_priority,
            "repeat": default_escalation_repeat,
            "role": "oncall-primary",
            "target": None,
            "template": default_template,
            "wait": default_escalation_wait,
        }
    ]
]
default_private_plan_steps = [
    [
        {
            "priority": default_priority,
            "repeat": default_repeat,
            "role": "oncall-primary",
            "target": None,
            "template": default_template,
            "wait": default_wait,
        }
    ]
]

ignore_plan_fields = [
    'created',
    'creator',
    'aggregation_reset',
    'aggregation_window',
    'id',
    'name',
    'plan_active.name',
    'plan_id',
    'threshold_count',
    'threshold_window',
    'tracking_key',
    'tracking_template',
    'tracking_type',
    'user_id',
    'type',
    'team_id',
    'all_steps',
]

stats_reset = {
    'sql_errors': 0,
    'users_added': 0,
    'users_failed_to_add': 0,
    'users_failed_to_update': 0,
    'users_purged': 0,
    'others_purged': 0,
    'teams_added': 0,
    'teams_failed_to_add': 0,
    'user_contacts_updated': 0,
}


priorities = {}    # name -> dict of info
target_roles = {}  # name -> id

# logging
logger = logging.getLogger()
formatter = logging.Formatter('%(asctime)s %(levelname)s %(name)s %(message)s')
log_file = os.environ.get('SYNC_TARGETS_LOG_FILE')
if log_file:
    ch = logging.handlers.RotatingFileHandler(log_file, mode='a', maxBytes=10485760, backupCount=10)
else:
    ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
ch.setFormatter(formatter)
logger.setLevel(logging.INFO)
logger.addHandler(ch)

pidfile = os.environ.get('SYNC_TARGETS_PIDFILE')
if pidfile:
    try:
        pid = os.getpid()
        with open(pidfile, 'w') as h:
            h.write('%s\n' % pid)
            logger.info('Wrote pid %s to %s', pid, pidfile)
    except IOError:
        logger.exception('Failed writing pid to %s', pidfile)


class DictDiffer(object):
    """
    Calculate the difference between two dictionaries as:
    (1) items added
    (2) items removed
    (3) keys same in both but changed values
    (4) keys same in both and unchanged values
    """
    def __init__(self, current_dict, past_dict):
        self.current_dict, self.past_dict = current_dict, past_dict
        self.set_current, self.set_past = set(current_dict.keys()), set(past_dict.keys())
        self.intersect = self.set_current.intersection(self.set_past)
    def added(self):
        return self.set_current - self.intersect
    def removed(self):
        return self.set_past - self.intersect
    def changed(self):
        return set(o for o in self.intersect if self.past_dict[o] != self.current_dict[o])
    def unchanged(self):
        return set(o for o in self.intersect if self.past_dict[o] == self.current_dict[o])


def cache_priorities(engine):
    global priorities
    connection = engine.raw_connection()
    cursor = connection.cursor(engine.dialect.dbapi.cursors.DictCursor)
    cursor.execute('''SELECT `priority`.`id`, `priority`.`name`, `priority`.`mode_id`
                      FROM `priority`''')
    priorities = {row['name']: row for row in cursor}
    cursor.close()
    connection.close()


def cache_target_roles(engine):
    global target_roles
    connection = engine.raw_connection()
    cursor = connection.cursor()
    cursor.execute('''SELECT `name`, `id` FROM target_role''')
    target_roles = dict(cursor)
    cursor.close()
    connection.close()


def normalize_phone_number(num):
    return format_number(parse(num.decode('utf-8'), 'US'),
                         PhoneNumberFormat.INTERNATIONAL)


def get_predefined_users(config):
    users = {}
    config_users = []

    try:
        config_users = config['sync_script']['preset_users']
    except KeyError:
        return {}

    for user in config_users:
        users[user['name']] = user
        for key in ('sms', 'call'):
            try:
                users[user['name']][key] = normalize_phone_number(users[user['name']][key])
            except (NumberParseException, KeyError, AttributeError):
                users[user['name']][key] = None

    return users


def prune_target(engine, target_name, target_type):
    if target_type == 'user':
        metrics.incr('users_purged')
    else:
        metrics.incr('others_purged')

    try:
        engine.execute('''DELETE FROM `target` WHERE `name` = %s AND `type_id` = (SELECT `id` FROM `target_type` WHERE `name` = %s)''', (target_name, target_type))
        logger.info('Deleted inactive target %s', target_name)

    # The user has messages or some other user data which should be preserved.
    # Just mark as inactive.
    except IntegrityError:
        logger.info('Marking target %s inactive', target_name)
        engine.execute('''UPDATE `target` SET `active` = FALSE WHERE `name` = %s AND `type_id` = (SELECT `id` FROM `target_type` WHERE `name` = %s)''', (target_name, target_type))

    except SQLAlchemyError as e:
        logger.error('Deleting target %s failed: %s', target_name, e)
        metrics.incr('sql_errors')


def fetch_teams_from_oncall(oncall_base_url):
    try:
        return requests.get('%s/api/v0/teams?fields=name&active=1' % oncall_base_url).json()
    except (ValueError, requests.exceptions.RequestException):
        logger.exception('Failed hitting oncall endpoint to fetch list of team names')
        return []


def fetch_users_from_oncall(oncall_base_url):
    oncall_user_endpoint = oncall_base_url + '/api/v0/users?fields=name&fields=contacts&fields=active'
    try:
        return {user['name']: fix_user_contacts(user['contacts'])
                for user in requests.get(oncall_user_endpoint).json()
                if user['active']}
    except (ValueError, KeyError, requests.exceptions.RequestException):
        logger.exception('Failed hitting oncall endpoint to fetch list of users')
        return {}


def fix_user_contacts(contacts):
    sms = contacts.get('sms')
    if sms:
        contacts['sms'] = normalize_phone_number(sms)

    call = contacts.get('call')
    if call:
        contacts['call'] = normalize_phone_number(call)

    return contacts


def ts_to_sql_datetime(ts):
    return 'FROM_UNIXTIME(%s)' % ts


def gen_where_filter_clause(connection, filters, filter_types, kwargs):
    '''
    How each where clauses are generated:
        1. find out column part through filters[col], skipping nonexistent columns that
           might exist from invalid 'fields' parameters
        2. find out operator part through operators[op]
        3. escape value through connection.escape(filter_types.get(col, str)(value))
        4. (optional) transform escaped value through filter_escaped_value_transforms[col](value)
    '''

    filter_escaped_value_transforms = {
        'updated': ts_to_sql_datetime,
        'created': ts_to_sql_datetime,
        'sent': ts_to_sql_datetime,
    }

    operators = {
        '': '%s = %s',
        'eq': '%s = %s',
        'in': '%s in %s',
        'ne': '%s != %s',
        'gt': '%s > %s',
        'ge': '%s >= %s',
        'lt': '%s < %s',
        'le': '%s <= %s',
        'contains': '%s LIKE CONCAT("%%%%", %s, "%%%%")',
        'startswith': '%s LIKE CONCAT(%s, "%%%%")',
        'endswith': '%s LIKE CONCAT("%%%%", %s)',
    }

    where = []
    for key, values in kwargs.iteritems():
        col, _, op = key.partition('__')
        # Skip columns that don't exist
        if col not in filters:
            continue
        col_type = filter_types.get(col, str)
        # Format strings because Falcon splits on ',' but not on '%2C'
        # TODO: Get rid of this by setting request options on Falcon 1.1
        if isinstance(values, basestring):
            values = values.split(',')
        for val in values:
            try:
                if op == 'in':
                    if len(values) == 1:
                        op = 'eq'
                        val = col_type(values[0])
                    else:
                        val = tuple([col_type(v) for v in values])
                else:
                    val = col_type(val)
            except (ValueError, TypeError):
                raise HTTPBadRequest('invalid argument type',
                                     '%s should be %s' % (col, col_type))
            val = connection.escape(val)
            if col in filter_escaped_value_transforms:
                val = filter_escaped_value_transforms[col](val)
            where.append(operators[op] % (filters[col], val))
    return where


def deactivate_plan(engine, plan_id):
    plan_query = '''DELETE FROM `plan_active` WHERE `plan_id`=%s'''

    query = plan_query % plan_id
    engine.execute(query)


def get_plan_id(engine, plan_name):
    plan_query = '''SELECT `id` FROM `plan`
    WHERE `name`="%s"'''

    query = plan_query % plan_name

    connection = engine.raw_connection()

    cursor = connection.cursor(engine.dialect.dbapi.cursors.DictCursor)
    cursor.execute(query)
    plan_id = cursor.fetchone()

    connection.close()

    if not plan_id:
        return None

    return plan_id['id']


def get_plan(engine, plan_name):
    plan_query = '''SELECT * FROM `plan`
    LEFT OUTER JOIN `plan_active` ON `plan`.`id` = `plan_active`.`plan_id`
    WHERE (`plan`.`name`="%s" AND `plan_active`.`name` IS NOT NULL)'''

    query = plan_query % plan_name

    connection = engine.raw_connection()

    cursor = connection.cursor(engine.dialect.dbapi.cursors.DictCursor)
    cursor.execute(query)
    plan = cursor.fetchone()

    connection.close()

    if not plan:
        return False

    return plan


# Used for comparison to intended plan nofitications
# filters ignored fields
def get_plan_notification(engine, plan_id):
    plan_notification_query = '''SELECT * FROM `plan_notification` WHERE (`plan_id`=%s)'''

    query = plan_notification_query % plan_id

    connection = engine.raw_connection()

    cursor = connection.cursor(engine.dialect.dbapi.cursors.DictCursor)
    cursor.execute(query)
    plan_notification = cursor.fetchall()

    connection.close()

    if not plan_notification:
        return False

    ignore_notification_fields = ['id', 'plan_id', 'dynamic_index']
    for idx, notification in enumerate(plan_notification):
        # filter fields we don't care about for comparison
        for field in ignore_notification_fields:
            plan_notification[idx].pop(field)
        # transform fields that create_plan already handles
        target_id = plan_notification[idx].pop('target_id')
        plan_notification[idx]['target'] = target_id
        priority_id = plan_notification[idx].pop('priority_id')
        plan_notification[idx]['priority'] = get_priority_id(engine, priority_id)
        role_id = plan_notification[idx].pop('role_id')
        plan_notification[idx]['role'] = get_target_role_name(engine, role_id)

    # get_plan_notification gives us a simple list of notifications
    # we need one with sublists, one per step level for comparison
    # but with step numbers removed (nested structure implies steplevel)
    plan_notification_clean = []
    unique_steps = set()
    for steps in plan_notification:
        unique_steps.add(steps['step'])
    for steplevel in unique_steps:
        current_steps = []
        current_steps = [steps for steps in plan_notification if steps['step'] == steplevel]
        current_steps_clean = copy.deepcopy(current_steps)
        for step in current_steps_clean:
            del(step['step'])
        plan_notification_clean.append(current_steps_clean)

    return plan_notification_clean


def get_priority_id(engine, priority_id):
    priority_query = '''SELECT * FROM `priority` WHERE `id`="%s"'''

    query = priority_query % priority_id

    connection = engine.raw_connection()

    cursor = connection.cursor(engine.dialect.dbapi.cursors.DictCursor)
    cursor.execute(query)
    priority = cursor.fetchone()

    connection.close()

    if not priority:
        return False

    priority_name = priority['name']

    return priority_name


def get_priority(engine, priority_name):
    priority_query = '''SELECT * FROM `priority` WHERE `name`="%s"'''

    query = priority_query % priority_name

    connection = engine.raw_connection()

    cursor = connection.cursor(engine.dialect.dbapi.cursors.DictCursor)
    cursor.execute(query)
    priority = cursor.fetchone()

    connection.close()

    if not priority:
        return False

    priority_id = priority['id']

    return priority_id


def get_priority(engine, priority_name):
    priority_query = '''SELECT * FROM `priority` WHERE `name`="%s"'''

    query = priority_query % priority_name

    connection = engine.raw_connection()

    cursor = connection.cursor(engine.dialect.dbapi.cursors.DictCursor)
    cursor.execute(query)
    priority = cursor.fetchone()

    connection.close()

    if not priority:
        return False

    priority_id = priority['id']

    return priority_id


def get_target_role_name(engine, role_id):
    role_query = '''SELECT * FROM `target_role` WHERE `id`="%s"'''

    query = role_query % role_id

    connection = engine.raw_connection()

    cursor = connection.cursor(engine.dialect.dbapi.cursors.DictCursor)
    cursor.execute(query)
    role = cursor.fetchone()

    connection.close()

    if not role:
        return False

    role_name = role['name']

    return role_name


def get_target_role(engine, role_name):
    role_query = '''SELECT * FROM `target_role` WHERE `name`="%s"'''

    query = role_query % role_name

    connection = engine.raw_connection()

    cursor = connection.cursor(engine.dialect.dbapi.cursors.DictCursor)
    cursor.execute(query)
    role = cursor.fetchone()

    connection.close()

    if not role:
        return False

    role_id = role['id']

    return role_id


def get_target(engine, target):
    # type_id=2 is "team"
    target_query = '''SELECT * FROM `target` WHERE `name`="%s" AND `type_id`=2'''

    query = target_query % target

    connection = engine.raw_connection()

    cursor = connection.cursor(engine.dialect.dbapi.cursors.DictCursor)
    cursor.execute(query)
    target = cursor.fetchone()

    connection.close()

    if not target:
        return False

    target_id = target['id']

    return target_id


def valid_plan(engine, plan, team, default_plan, plan_type):
    is_plan = clean_plan(plan)
    should_plan = clean_plan(default_plan)
    retval = True
    errormsg = ""

    should_plan['step_count'] = 1
    team_target_id = get_target(engine, default_plan['all_steps'][0][0]['target'])
    hours = plan_type.split('-')[-1]

    if plan_type == 'withsrt-24x7' or plan_type == 'withsrt-businesshours':
        should_plan['step_count'] = 3
        should_plan['description'] = 'Escalation and after hours support by SRT, EOD and MOD'

        # plan notification expected values
        eod_target_id = get_target(engine, default_plan['all_steps'][0][1]['target'])
        srt_target_id = get_target(engine, default_plan['all_steps'][1][0]['target'])
        mod_target_id = get_target(engine, default_plan['all_steps'][2][0]['target'])

        should_notification = copy.deepcopy(default_scrumteam_plan_steps)
        should_notification[0][0]['target'] = team_target_id
        should_notification[0][1]['target'] = eod_target_id
        should_notification[1][0]['target'] = srt_target_id
        should_notification[1][1]['target'] = eod_target_id
        should_notification[2][0]['target'] = mod_target_id
    elif plan_type == 'withmod-standby' or plan_type == 'withmod-businesshours':
        should_plan['step_count'] = 3
        should_plan['description'] = 'Standby plan with escalation (24x7)'

        mod_target_id = get_target(engine, default_plan['all_steps'][1][0]['target'])

        should_notification = copy.deepcopy(default_standby_plan_steps)
        should_notification[0][0]['target'] = team_target_id
        should_notification[1][0]['target'] = mod_target_id
        should_notification[2][0]['target'] = mod_target_id
    elif plan_type == 'private-workhours':
        should_plan['description'] = 'Simple plan without escalation ({})'.format(hours)

        should_notification = copy.deepcopy(default_private_plan_steps)
        should_notification[0][0]['target'] = team_target_id
    elif plan_type == 'private-businesshours':
        should_plan['description'] = 'Simple plan without escalation ({})'.format(hours)

        should_notification = copy.deepcopy(default_private_plan_steps)
        should_notification[0][0]['target'] = team_target_id
    elif plan_type == 'private-24x7':
        should_plan['description'] = 'Simple plan without escalation ({})'.format(hours)

        should_notification = copy.deepcopy(default_private_plan_steps)
        should_notification[0][0]['target'] = team_target_id
    else:
        logger.error('Unknown plan type: %s for plan id: %s', plan_type, plan['id'])
        sys.exit(1)

    # compare plan metadata
    if not same_plan(should_plan, is_plan):
        retval = False

    # compare plan_notifications
    is_notification = get_plan_notification(engine, plan['id'])
    if not is_notification:
        errormsg = "No plan notifications found for: {}".format(plan['id'])
        retval = False
    else:
        for steplevel, list_of_steps in enumerate(should_notification):
            if not same_steps(list_of_steps, is_notification[steplevel]):
                retval = False

    if not retval and errormsg:
        logger.info(errormsg)

    return retval


# remove fields ignored for comparison
def clean_plan(plan):
    clean_plan = copy.deepcopy(plan)
    for field in ignore_plan_fields:
        try:
            clean_plan.pop(field)
        except KeyError:
            pass
    return clean_plan


def same_plan(is_plan, should_plan):
    if not cmp(should_plan, is_plan) == 0:
        logger.info("Got: %s", is_plan)
        logger.info("Should be: %s", should_plan)
        return False
    return True


def same_steps(is_step, should_step):
    # we don't care about ordering, so we reverse the check as well
    if should_step != is_step and should_step[::-1] != is_step:
        logger.info("not the same")
        logger.info("should: %s", should_step)
        logger.info("is: %s", is_step)
        return False
    return True


# get all iris plans
def fetch_plans(engine):
    plan_query = '''SELECT %s FROM `plan` JOIN `target` ON `plan`.`user_id` = `target`.`id`
    LEFT OUTER JOIN `plan_active` ON `plan`.`id` = `plan_active`.`plan_id`'''

    plan_columns = {
        'id': '`plan`.`id` as `id`',
        'name': '`plan`.`name` as `name`',
        'threshold_window': '`plan`.`threshold_window` as `threshold_window`',
        'threshold_count': '`plan`.`threshold_count` as `threshold_count`',
        'aggregation_window': '`plan`.`aggregation_window` as `aggregation_window`',
        'aggregation_reset': '`plan`.`aggregation_reset` as `aggregation_reset`',
        'tracking_type': '`plan`.`tracking_type` as `tracking_type`',
        'tracking_key': '`plan`.`tracking_key` as `tracking_key`',
        'tracking_template': '`plan`.`tracking_template` as `tracking_template`',
        'description': '`plan`.`description` as `description`',
        'created': 'UNIX_TIMESTAMP(`plan`.`created`) as `created`',
        'creator': '`target`.`name` as `creator`',
        'active': 'IF(`plan_active`.`plan_id` IS NULL, FALSE, TRUE) as `active`',
    }

    query = plan_query % ', '.join(plan_columns[f] for f in plan_columns)

    where = []
    where.append('`plan_active`.`plan_id` IS NOT NULL')

    connection = engine.raw_connection()
    # where += gen_where_filter_clause(
    #     connection, plan_filters, plan_filter_types, req.params)

    query = query + ' WHERE ' + ' AND '.join(where)

    cursor = connection.cursor(engine.dialect.dbapi.cursors.DictCursor)
    cursor.execute(query)

    connection.close()


def get_default_plan_steps(team, plan_type, target_team, standby_team, escalation_team, standby_escalation_team):
    plan_description = ""
    step_count = 1
    all_steps = []
    if plan_type == 'withsrt-24x7':
        if not standby_team:
            logger.error("Missing standbyteam for plan %s", team)
        if not escalation_team:
            logger.error("Missing escalation_team for plan %s", team)
        if not standby_escalation_team:
            logger.error("Missing standby_escalation_team for plan %s", team)

        plan_description = 'Escalation and after hours support by SRT, EOD and MOD'
        step_count = 3
        all_steps = copy.deepcopy(default_scrumteam_plan_steps)
        all_steps[0][0]['target'] = target_team
        all_steps[0][1]['target'] = standby_team
        all_steps[1][0]['target'] = escalation_team
        all_steps[1][1]['target'] = standby_team
        all_steps[2][0]['target'] = standby_escalation_team
    elif plan_type == 'withmod-standby':
        if not escalation_team:
            logger.error("Missing escalation_team for plan %s", team)
        plan_description = 'Standby plan with escalation (24x7)'
        step_count = 3
        all_steps = copy.deepcopy(default_standby_plan_steps)
        all_steps[0][0]['target'] = target_team
        all_steps[1][0]['target'] = escalation_team
        all_steps[2][0]['target'] = escalation_team
    elif plan_type in ['private-workhours', 'private-businesshours', 'private-24x7']:
        all_steps = copy.deepcopy(default_private_plan_steps)
        all_steps[0][0]['target'] = target_team
        plan_description = 'Simple plan without escalation (' + plan_type.split('-')[-1] + ')'
    else:
        logger.warn("No such plan type %s for %s", plan_type, team)
    return plan_description, step_count, all_steps

# Provision a default scrum team plan
def create_plan(engine, team, plan_name, all_steps, step_count, plan_description):
    logger.info("Creating default plan %s for %s", plan_name, team)
    insert_plan_query = '''REPLACE INTO `plan` (
        `user_id`, `name`, `created`, `description`, `step_count`,
        `threshold_window`, `threshold_count`, `aggregation_window`,
        `aggregation_reset`, `tracking_key`, `tracking_type`, `tracking_template`
    ) VALUES (
        (SELECT `id` FROM `target` where `name` = :creator AND `type_id` = (
        SELECT `id` FROM `target_type` WHERE `name` = 'user'
        )),
        :name,
        :created,
        :description,
        :step_count,
        :threshold_window,
        :threshold_count,
        :aggregation_window,
        :aggregation_reset,
        :tracking_key,
        :tracking_type,
        :tracking_template
    )'''

    insert_plan_step_query = '''REPLACE INTO `plan_notification` (
        `plan_id`, `step`, `priority_id`, `target_id`, `template`, `role_id`, `repeat`, `wait`
    ) VALUES (
        :plan_id,
        :step,
        :priority_id,
        (SELECT `target`.`id` FROM `target` WHERE `target`.`name` = :target AND `target`.`type_id` =
        (SELECT `target_role`.`type_id` FROM `target_role` WHERE `id` = :role_id)
        ),
        :template,
        :role_id,
        :repeat,
        :wait
    )'''

    get_allowed_roles_query = '''SELECT `target_role`.`id`
                                FROM `target_role`
                                JOIN `target_type` ON `target_type`.`id` = `target_role`.`type_id`
                                JOIN `target` ON `target`.`type_id` = `target_type`.`id`
                                WHERE `target`.`name` = :target'''

    plan_dict = copy.deepcopy(default_plan_template)
    plan_dict['name'] = plan_name
    plan_dict['created'] = datetime.datetime.utcnow()
    plan_dict['description'] = plan_description
    plan_dict['step_count'] = step_count

    Session = sessionmaker(bind=engine)
    session = Session()
    plan_id = session.execute(insert_plan_query, plan_dict).lastrowid

    for index, steps in enumerate(all_steps, start=1):
        for step in steps:
            step['plan_id'] = plan_id
            step['step'] = index

            role = target_roles.get(step.get('role'))
            step['role_id'] = role

            priority = priorities.get(step['priority'])
            step['priority_id'] = priority['id']

            allowed_roles = {row[0] for row in session.execute(get_allowed_roles_query, step)}

            if not allowed_roles:
                session.close()
                logger.error("For team %s", team)
                logger.error("No result for: %s", get_allowed_roles_query)
                logger.error("With input: %s", step)
                raise HTTPBadRequest(
                    'Invalid plan',
                    'Target %s not found for step %s' % (step['target'], index))

            if role not in allowed_roles:
                session.close()
                raise HTTPBadRequest(
                    'Invalid role',
                    'Role %s is not appropriate for target %s in step %s' % (
                        step['role'], step['target'], index))

            try:
                session.execute(insert_plan_step_query, step)
            except IntegrityError:
                session.close()
                raise HTTPBadRequest('Invalid plan',
                                     'Target not found for step %s' % index)

    session.execute('INSERT INTO `plan_active` (`name`, `plan_id`) '
                    'VALUES (:name, :plan_id) ON DUPLICATE KEY UPDATE `plan_id`=:plan_id',
                    {'name': plan_name, 'plan_id': plan_id})

    session.commit()
    session.close()


def get_users(engine):
    iris_users = {}
    for row in engine.execute('''SELECT `target`.`name` as `name`, `mode`.`name` as `mode`,
                                        `target_contact`.`destination`
                                 FROM `target`
                                 JOIN `user` on `target`.`id` = `user`.`target_id`
                                 LEFT OUTER JOIN `target_contact` ON `target`.`id` = `target_contact`.`target_id`
                                 LEFT OUTER JOIN `mode` ON `target_contact`.`mode_id` = `mode`.`id`
                                 WHERE `target`.`active` = TRUE
                                 ORDER BY `target`.`name`'''):
        contacts = iris_users.setdefault(row.name, {})
        if row.mode is None or row.destination is None:
            continue
        contacts[row.mode] = row.destination
    return iris_users


def insert_user(engine, username, target_types, modes, oncall_users):
    user_add_sql = 'INSERT IGNORE INTO `user` (`target_id`) VALUES (%s)'
    target_contact_add_sql = '''INSERT INTO `target_contact` (`target_id`, `mode_id`, `destination`)
                                VALUES (%s, %s, %s)
                                ON DUPLICATE KEY UPDATE `destination` = %s'''
    target_add_sql = 'INSERT INTO `target` (`name`, `type_id`) VALUES (%s, %s) ON DUPLICATE KEY UPDATE `active` = TRUE'


    logger.info('Inserting user target %s' % username)
    try:
        target_id = engine.execute(target_add_sql, (username, target_types['user'])).lastrowid
        engine.execute(user_add_sql, (target_id, ))
    except SQLAlchemyError as e:
        metrics.incr('users_failed_to_add')
        metrics.incr('sql_errors')
        logger.exception('Failed to add user %s' % username)
        return

    metrics.incr('users_added')
    for key, value in oncall_users[username].iteritems():
        if value and key in modes:
            logger.info('%s: %s -> %s' % (username, key, value))
            engine.execute(target_contact_add_sql, (target_id, modes[key], value, value))


def insert_team(engine, team, target_types):
    target_add_sql = 'INSERT INTO `target` (`name`, `type_id`) VALUES (%s, %s) ON DUPLICATE KEY UPDATE `active` = TRUE'

    logger.info('Inserting team target %s' % team)
    try:
        target_id = engine.execute(target_add_sql, (team, target_types['team'])).lastrowid
        metrics.incr('teams_added')
    except SQLAlchemyError as e:
        logger.exception('Error inserting team %s: %s' % (team, e))
        metrics.incr('teams_failed_to_add')


def update_user(engine, username, iris_users, oncall_users, modes):
    contact_update_sql = 'UPDATE target_contact SET destination = %s WHERE target_id = (SELECT id FROM target WHERE name = %s) AND mode_id = %s'
    contact_insert_sql = 'INSERT INTO target_contact (target_id, mode_id, destination) VALUES ((SELECT id FROM target WHERE name = %s), %s, %s)'
    contact_delete_sql = 'DELETE FROM target_contact WHERE target_id = (SELECT id FROM target WHERE name = %s) AND mode_id = %s'

    try:
        db_contacts = iris_users[username]
        oncall_contacts = oncall_users[username]
        for mode in modes:
            if mode in oncall_contacts and oncall_contacts[mode]:
                if mode in db_contacts:
                    if oncall_contacts[mode] != db_contacts[mode]:
                        logger.info('%s: updating %s' % (username, mode))
                        metrics.incr('user_contacts_updated')
                        engine.execute(contact_update_sql, (oncall_contacts[mode], username, modes[mode]))
                else:
                    logger.info('%s: adding %s' % (username, mode))
                    metrics.incr('user_contacts_updated')
                    engine.execute(contact_insert_sql, (username, modes[mode], oncall_contacts[mode]))
            elif mode in db_contacts:
                logger.info('%s: deleting %s' % (username, mode))
                metrics.incr('user_contacts_updated')
                engine.execute(contact_delete_sql, (username, modes[mode]))
            else:
                logger.debug('%s: missing %s' % (username, mode))
    except SQLAlchemyError as e:
        metrics.incr('users_failed_to_update')
        metrics.incr('sql_errors')
        logger.exception('Failed to update user %s' % username)
        return


# Return default plans for teams we manage
def get_default_plans(space_to_srt_mapping, team, platform_teams, standby_teams, standby_escalation_teams, scrumteams):
    plans = []

    sane_name = team.replace(immutable_suffix, "")
    bol_teamname = sane_name.split('-')[0]

    private_24x7_planname = bol_teamname + '-24x7' + private_suffix + immutable_suffix
    private_24x7_teamname = bol_teamname + '-24x7' + immutable_suffix
    private_workhours_planname = bol_teamname + '-workhours' + private_suffix + immutable_suffix
    private_workhours_teamname = bol_teamname + '-workhours' + immutable_suffix
    private_businesshours_planname = bol_teamname + '-businesshours' + private_suffix + immutable_suffix
    private_businesshours_teamname = bol_teamname + '-businesshours' + immutable_suffix

    if bol_teamname in platform_teams:
        # private 24x7 and private workhours are default
        pass
    elif bol_teamname in standby_teams:
        # with mod 24x7
        withmod_planname = bol_teamname + '-24x7' + mod_suffix + immutable_suffix
        withmod_teamname = bol_teamname + '-workhours' + immutable_suffix
        escalation_team = mod_team + '-24x7' + immutable_suffix

        plan_description, step_count, all_steps = get_default_plan_steps(team, 'withmod-standby', withmod_teamname, None, escalation_team, None)
        plans.append({
            'name': withmod_planname,
            'description': plan_description,
            'step_count': step_count,
            'all_steps': all_steps,
            'type': 'withmod-standby',
        })

        # with mod businesshours
        withmod_businesshours_planname = bol_teamname + '-businesshours' + mod_suffix + immutable_suffix
        withmod_businesshours_teamname = bol_teamname + '-workhours' + immutable_suffix
        escalation_team = mod_team + '-businesshours' + immutable_suffix
        plan_description, step_count, all_steps = get_default_plan_steps(team, 'withmod-standby', withmod_businesshours_teamname, None, escalation_team, None)
        plans.append({
            'name': withmod_businesshours_planname,
            'description': plan_description,
            'step_count': step_count,
            'all_steps': all_steps,
            'type': 'withmod-businesshours',
        })
    elif bol_teamname in standby_escalation_teams:
        # private 24x7 is default
        pass
    elif team.startswith(scrumteam_prefix):
        if bol_teamname in scrumteams:
            # lookup supporting srt
            space = scrumteams[bol_teamname]['space']
            srt = space_to_srt_mapping[space] + immutable_suffix

            # with srt and eod, with escalation to mod 24x7
            withsrt_planname = bol_teamname + '-24x7' + srt_suffix + immutable_suffix
            withsrt_teamname = bol_teamname + '-workhours' + immutable_suffix
            escalation_team = srt
            standby_team = middleware_team + '-standby' + immutable_suffix
            standby_escalation_team = mod_team + '-standby' + immutable_suffix

            plan_description, step_count, all_steps = get_default_plan_steps(team, 'withsrt-24x7', withsrt_teamname, standby_team, escalation_team, standby_escalation_team)
            plans.append({
                'name': withsrt_planname,
                'description': plan_description,
                'step_count': step_count,
                'all_steps': all_steps,
                'type': 'withsrt-24x7',
            })

            # with srt and eod, with escalation to mod businesshours
            withsrt_businesshours_planname = bol_teamname + '-businesshours' + srt_suffix + immutable_suffix
            withsrt_businesshours_teamname = bol_teamname + '-workhours' + immutable_suffix
            escalation_team = srt
            standby_team = middleware_team + '-businesshours' + immutable_suffix
            standby_escalation_team = mod_team + '-businesshours' + immutable_suffix
            plan_description, step_count, all_steps = get_default_plan_steps(team, 'withsrt-24x7', withsrt_businesshours_teamname, standby_team, escalation_team, standby_escalation_team)
            plans.append({
                'name': withsrt_businesshours_planname,
                'description': plan_description,
                'step_count': step_count,
                'all_steps': all_steps,
                'type': 'withsrt-businesshours',
            })

        # private 24x7, businesshours and workhours
        plan_description, step_count, all_steps = get_default_plan_steps(team, 'private-24x7', private_24x7_teamname, None, None, None)
        plans.append({
            'name': private_24x7_planname,
            'description': plan_description,
            'step_count': step_count,
            'all_steps': all_steps,
            'type': 'private-24x7',
        })
        plan_description, step_count, all_steps = get_default_plan_steps(team, 'private-businesshours', private_businesshours_teamname, None, None, None)
        plans.append({
            'name': private_businesshours_planname,
            'description': plan_description,
            'step_count': step_count,
            'all_steps': all_steps,
            'type': 'private-businesshours',
        })
        plan_description, step_count, all_steps = get_default_plan_steps(team, 'private-workhours', private_workhours_teamname, None, None, None)
        plans.append({
            'name': private_workhours_planname,
            'description': plan_description,
            'step_count': step_count,
            'all_steps': all_steps,
            'type': 'private-workhours',
        })

    return plans


def sync_from_oncall(config, engine, purge_old_users=True):
    # before we start, make sure we can open teams.yaml
    scrumteams = {}
    teamsfile = config['sync_script']['scrumteams_file']
    with open(teamsfile, 'r') as stream:
        try:
            scrumteams = yaml.load(stream)
        except yaml.YAMLError as err:
            logger.error(err)

    # users and teams present in our oncall database
    oncall_base_url = config.get('oncall-api')

    if not oncall_base_url:
        logger.error('Missing URL to oncall-api, which we use for user/team lookups. Bailing.')
        return

    oncall_users = fetch_users_from_oncall(oncall_base_url)
    if not oncall_users:
        logger.warning('No users found. Bailing.')
        return
    metrics.set('users_found', len(oncall_users))

    oncall_team_names = fetch_teams_from_oncall(oncall_base_url)
    if not oncall_team_names:
        logger.warning('We do not have a list of team names')

    oncall_team_names = set(oncall_team_names)
    metrics.set('teams_found', len(oncall_team_names))

    session = sessionmaker(bind=engine)()

    # users present in iris' database
    iris_users = get_users(engine)
    iris_usernames = iris_users.viewkeys()

    # users from the oncall endpoints and config files
    oncall_users.update(get_predefined_users(config))
    oncall_usernames = oncall_users.viewkeys()

    # set of users not presently in iris
    users_to_insert = oncall_usernames - iris_usernames
    # set of existing iris users that are in the user oncall database
    users_to_update = iris_usernames & oncall_usernames
    users_to_mark_inactive = iris_usernames - oncall_usernames

    # get objects needed for insertion
    target_types = {name: id for name, id in session.execute('SELECT `name`, `id` FROM `target_type`')}  # 'team' and 'user'
    modes = {name: id for name, id in session.execute('SELECT `name`, `id` FROM `mode`')}
    iris_team_names = {name for (name, ) in engine.execute('''SELECT `name` FROM `target` WHERE `type_id` = %s''', target_types['team'])}

    # insert users that need to be
    logger.info('Users to insert (%d)', len(users_to_insert))
    for username in users_to_insert:
        insert_user(engine, username, target_types, modes, oncall_users)

    # update users that need to be
    logger.info('Users to update (%d)', len(users_to_update))
    for username in users_to_update:
        update_user(engine, username, iris_users, oncall_users, modes)

    # sync teams between iris and oncall
    teams_to_insert = oncall_team_names - iris_team_names
    teams_to_update = oncall_team_names & iris_team_names
    teams_to_deactivate = iris_team_names - oncall_team_names

    ### Report
    logger.info("Teams (target) to insert: %s", list(teams_to_insert))
    logger.info("Teams (target) to check for updating: %s", len(teams_to_update))
    logger.info("Teams (target) to purge: %s", list(teams_to_deactivate))
    logger.info('Users to mark inactive: %s', list(users_to_mark_inactive))

    if NOOP:
        logger.info("No Op mode enabled. No changes made.")
        return

    # we need some context to understand which plans are created for which team
    platform_teams = config['sync_script']['platform_teams']
    standby_teams = config['sync_script']['standby_teams']
    standby_escalation_teams = config['sync_script']['standby_escalation_teams']
    space_to_srt_mapping = config['sync_script']['space_to_srt_team']

    # Insert teams as targets
    for team in teams_to_insert:
        insert_team(engine, team, target_types)

    # While all oncall teams are valid targets, we need a list of actual teams
    # in order to understand which plans need creating
    bol_teams = []
    for bol_team in teams_to_insert:
        bol_team = bol_team.replace(immutable_suffix, '')
        for timeperiod in timeperiods:
            bol_team = bol_team.replace('-' + timeperiod, '')
        bol_teams.append(bol_team)
    bol_teams = set(bol_teams)

    # Provision default plans for our new targets
    for team in bol_teams:
        for plan in get_default_plans(space_to_srt_mapping, team, platform_teams, standby_teams, standby_escalation_teams, scrumteams):
            create_plan(engine, team, plan['name'], plan['all_steps'], plan['step_count'], plan['description'])


    # Update plans
    for team in teams_to_update:
        for default_plan in get_default_plans(space_to_srt_mapping, team, platform_teams, standby_teams, standby_escalation_teams, scrumteams):
            actual_plan = get_plan(engine, default_plan['name'])
            if not actual_plan or not valid_plan(engine, actual_plan, team, default_plan, default_plan['type']):
                create_plan(engine, team, default_plan['name'], default_plan['all_steps'], default_plan['step_count'], default_plan['description'])


    # Mark users/teams/plans inactive
    if purge_old_users:
        for username in users_to_mark_inactive:
            prune_target(engine, username, 'user')
        for team in teams_to_deactivate:
            prune_target(engine, team, 'team')

            for plan in get_default_plans(space_to_srt_mapping, team, platform_teams, standby_teams, standby_escalation_teams, scrumteams):
                plan_id = get_plan_id(engine, plan['name'])
                if plan_id:
                    deactivate_plan(engine, plan_id)

    session.commit()
    session.close()


def main():
    global NOOP
    if len(sys.argv) == 3 and sys.argv[2] == '--noop':
        NOOP = True
    if len(sys.argv) > 3 or len(sys.argv) < 1:
        sys.exit('USAGE: %s [--noop]' % sys.argv[0])

    config = load_config()
    metrics.init(config, 'iris-sync-targets', stats_reset)

    default_nap_time = 3600

    try:
        nap_time = int(config.get('sync_script_nap_time', default_nap_time))
    except ValueError:
        nap_time = default_nap_time

    engine = create_engine(config['db']['conn']['str'] % config['db']['conn']['kwargs'],
                           **config['db']['kwargs'])

    # Initialize these to zero at the start of the app, and don't reset them at every
    # metrics interval
    metrics.set('users_found', 0)
    metrics.set('teams_found', 0)

    metrics_task = spawn(metrics.emit_forever)

    while True:
        cache_target_roles(engine)
        cache_priorities(engine)
        if not bool(metrics_task):
            logger.error('metrics task failed, %s', metrics_task.exception)
            metrics_task = spawn(metrics.emit_forever)

        sync_from_oncall(config, engine)

        logger.info('Sleeping for %d seconds' % nap_time)
        sleep(nap_time)


if __name__ == '__main__':
    main()
