# Copyright (c) LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
# See LICENSE in the project root for license information.

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

from iris.api import load_config
from iris import metrics


logging.getLogger('requests').setLevel(logging.WARNING)

immutable_prefix = 'ad-'
scrumteam_prefix = immutable_prefix + 'team'
srt_suffix = '-with-srt'
mod_suffix = '-with-mod'
private_suffix = '-private'
middleware_team = 'bol-tam'
mod_team = 'bol-mod'

default_template = "default"
default_wait = 1600
default_repeat = 0  # $repeat * $wait = escal time
default_escalation_repeat = 1
default_escalation_wait = 900
default_priority = "high"
default_standby_escalation_priority = "urgent"

ignore_plan_fields = ['created',
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


def fix_user_contacts(contacts):
    sms = contacts.get('sms')
    if sms:
        contacts['sms'] = normalize_phone_number(sms)

    call = contacts.get('call')
    if call:
        contacts['call'] = normalize_phone_number(call)

    return contacts


def fetch_users_from_oncall(oncall_base_url):
    oncall_user_endpoint = oncall_base_url + '/api/v0/users?fields=name&fields=contacts&fields=active'
    try:
        return {user['name']: fix_user_contacts(user['contacts'])
                for user in requests.get(oncall_user_endpoint).json()
                if user['active']}
    except (ValueError, KeyError, requests.exceptions.RequestException):
        logger.exception('Failed hitting oncall endpoint to fetch list of users')
        return {}


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

    return plan_notification


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


def valid_scrumteam_plan(engine, plan, team, plan_opts):
    # compare plans

    sane_plan = plan.copy()
    for field in ignore_plan_fields:
        sane_plan.pop(field)

    valid_plan = {
        u'description': u'Escalation and after hours support by SRT, EOD and MOD',
        u'step_count': 1,
        u'team_id': None,
    }

    if not cmp(valid_plan, sane_plan) == 0:
        logger.info("Plan %s needs fixing", plan['plan_active.name'])
        return False

    # compare plan_notifications
    plan_notification = get_plan_notification(engine, plan['id'])
    if not plan_notification:
        return False

    ignore_plan_notification_fields = ['id', 'plan_id']

    sane_plan_notification = plan_notification[:]
    for idx, notification in enumerate(sane_plan_notification):
        for field in ignore_plan_notification_fields:
            sane_plan_notification[idx].pop(field)

    prio_high_id = get_priority(engine, "high")
    prio_urgent_id = get_priority(engine, "urgent")
    oncall_primary_role_id = get_target_role(engine, "oncall-primary")
    oncall_primary_exclude_holidays_role_id = get_target_role(engine, "oncall-primary-exclude-holidays")
    team_target_id = get_target(engine, team)

    eod_target_id = get_target(engine, plan_opts['standby_team'])
    mod_target_id = get_target(engine, plan_opts['standby_escalation_team'])
    srt_target_id = get_target(engine, plan_opts['escalation_team'])

    template = 'default'
    valid_plan_notification = [
            {
                u'dynamic_index': None,
                u'priority_id': prio_high_id,
                u'repeat': default_repeat,
                u'role_id': oncall_primary_exclude_holidays_role_id,
                u'step': 1,
                u'target_id': team_target_id,
                u'template': template,
                u'wait': default_wait,
            },
            {   u'dynamic_index': None,
                u'priority_id': prio_high_id,
                u'repeat': default_repeat,
                u'role_id': oncall_primary_role_id,
                u'step': 1,
                u'target_id': eod_target_id,
                u'template': template,
                u'wait': default_wait,
            },
            {   u'dynamic_index': None,
                u'priority_id': prio_high_id,
                u'repeat': default_escalation_repeat,
                u'role_id': oncall_primary_exclude_holidays_role_id,
                u'step': 2,
                u'target_id': srt_target_id,
                u'template': template,
                u'wait': default_escalation_wait,
            },
            {   u'dynamic_index': None,
                u'priority_id': prio_urgent_id,
                u'repeat': default_escalation_repeat,
                u'role_id': oncall_primary_role_id,
                u'step': 2,
                u'target_id': eod_target_id,
                u'template': template,
                u'wait': default_escalation_wait,
            },
            {   u'dynamic_index': None,
                u'priority_id': prio_urgent_id,
                u'repeat': default_escalation_repeat,
                u'role_id': oncall_primary_role_id,
                u'step': 3,
                u'target_id': mod_target_id,
                u'template': template,
                u'wait': default_escalation_wait,
            }
        ]

    if not cmp(valid_plan_notification, sane_plan_notification) == 0:
        logger.info("Plan notification %s needs fixing", plan['plan_active.name'])
        for idx, _ in enumerate(valid_plan_notification):
            logger.info("Step %s", idx)
            d = DictDiffer(valid_plan_notification[idx], sane_plan_notification[idx])
            logger.info("Added: %s", d.added())
            logger.info("Removed: %s", d.removed())
            logger.info("Changed: %s", d.changed())
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


# Provision a default scrum team plan
def create_plan(engine, team, plan_name, plan_type, extra_opts):
    logger.info("Creating default %s plan for %s", plan_type, team)
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

    now = datetime.datetime.utcnow()

    plan_description = ""
    step_count = 0
    all_steps = []
    if plan_type == 'scrumteam':
        plan_description = 'Escalation and after hours support by SRT, EOD and MOD'
        step_count = 1
        all_steps = [
            [
                {
                    "priority": default_priority,
                    "repeat": default_repeat,
                    "role": "oncall-primary-exclude-holidays",
                    "target": team,
                    "template": default_template,
                    "wait": default_wait,
                },
                {
                    "priority": default_priority,
                    "repeat": default_repeat,
                    "role": "oncall-primary",
                    "target": extra_opts['standby_team'],
                    "template": default_template,
                    "wait": default_wait,
                }
            ],
            [
                {
                    "priority": default_priority,
                    "repeat": default_escalation_repeat,
                    "role": "oncall-primary-exclude-holidays",
                    "target": extra_opts['escalation_team'],
                    "template": default_template,
                    "wait": default_escalation_wait,
                },
                {
                    "priority": default_standby_escalation_priority,
                    "repeat": default_escalation_repeat,
                    "role": "oncall-primary",
                    "target": extra_opts['standby_team'],
                    "template": default_template,
                    "wait": default_escalation_wait,
                }
            ],
            [
                {
                    "priority": default_standby_escalation_priority,
                    "repeat": default_escalation_repeat,
                    "role": "oncall-primary",
                    "target": extra_opts['standby_escalation_team'],
                    "template": default_template,
                    "wait": default_escalation_wait,
                }
            ]
        ]
    elif plan_type == 'standby':
        plan_description = 'Standby plan with escalation'
        step_count = 0
        all_steps = [
            [
                {
                    "priority": default_priority,
                    "repeat": default_repeat,
                    "role": "oncall-primary",
                    "target": team,
                    "template": default_template,
                    "wait": default_wait,
                }
            ],
            [
                {
                    "priority": default_standby_escalation_priority,
                    "repeat": default_escalation_repeat,
                    "role": "oncall-primary",
                    "target": team,
                    "template": default_template,
                    "wait": default_escalation_wait,
                }
            ],
            [
                {
                    "priority": default_standby_escalation_priority,
                    "repeat": default_escalation_repeat,
                    "role": "oncall-primary",
                    "target": extra_opts['escalation_team'],
                    "template": default_template,
                    "wait": default_escalation_wait,
                }
            ]
        ]
    elif plan_type == 'private':
        plan_description = 'Simple plan without escalation'
        step_count = 0
        all_steps = [
            [
                {
                    "priority": default_priority,
                    "repeat": default_repeat,
                    "role": "oncall-primary",
                    "target": team,
                    "template": default_template,
                    "wait": default_wait,
                }
            ]
        ]
    else:
        logger.info("Unknown plan type '%s' requested", plan_type)

    plan_dict = {
        'creator': 'dummy',
        'name': plan_name,
        'created': now,
        'description': plan_description,
        'step_count': step_count,
        'threshold_window': 900,
        'threshold_count': 10,
        'aggregation_window': 300,
        'aggregation_reset': 300,
        'tracking_key': None,
        'tracking_type': None,
        'tracking_template': None
    }

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
                logger.warn("No result for: %s", get_allowed_roles_query)
                logger.warn("With input: %s", step)
                logger.warn("extra_opts: %s", extra_opts)
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


def get_teams_default_plans(config, team):
    sane_team = team.replace(immutable_prefix, "")

    platform_teams = config['sync_script']['platform_teams']
    standby_teams = config['sync_script']['standby_teams']
    standby_escalation_teams = config['sync_script']['standby_escalation_teams']
    teamsfile = config['sync_script']['scrumteams_file']

    scrumteams = {}
    with open(teamsfile, 'r') as stream:
        try:
            scrumteams = yaml.load(stream)
        except yaml.YAMLError as err:
            logger.info(err)

    plans = []
    if team.startswith(scrumteam_prefix):
        plans.append({'name': team + private_suffix, 'type': 'private', 'extra_opts': {}})

        # lookup supporting srt
        space_to_srt_mapping = config['sync_script']['space_to_srt_team']
        if sane_team in scrumteams:
            space = scrumteams[sane_team]['space']
            srt = immutable_prefix + space_to_srt_mapping[space]
            plans.append({'name': team + srt_suffix, 'type': 'scrumteam', 'extra_opts': {
                'escalation_team': srt,
                'standby_team': immutable_prefix + middleware_team,
                'standby_escalation_team': immutable_prefix + mod_team,
            }})
        else:
            logger.warn("Skipping srt supported plan Creation. Couldn't find team %s in scrumteam hash", sane_team)

    elif sane_team in platform_teams:
        plans.append({'name': team + private_suffix, 'type': 'private', 'extra_opts': {}})
    elif sane_team in standby_teams:
        plans.append({'name': team + mod_suffix, 'type': 'standby', 'extra_opts': {
            'escalation_team': immutable_prefix + mod_team,
        }})
        plans.append({'name': team + private_suffix, 'type': 'private', 'extra_opts': {}})
    elif sane_team in standby_escalation_teams:
        plans.append({'name': team + private_suffix, 'type': 'private', 'extra_opts': {}})
    else:
        plans.append({'name': team + private_suffix, 'type': 'private', 'extra_opts': {}})

    return plans


def sync_from_oncall(config, engine, purge_old_users=True):
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

    logger.info('Teams to insert (%d)', len(teams_to_insert))
    for team in teams_to_insert:
        insert_team(engine, team, target_types)

    # provision default plans
    logger.info('Plans to insert (%d)', len(teams_to_insert))
    for team in teams_to_insert:
        for plan in get_teams_default_plans(config, team):
            create_plan(engine, team, plan['name'], plan['type'], plan['extra_opts'])

    logger.info('Plans to check (%d)', len(teams_to_insert))
    for team in teams_to_update:
        for plan in get_teams_default_plans(config, team):
            if plan['type'] == 'scrumteam':
                plan_dict = get_plan(engine, plan['name'])
                if not plan_dict or not valid_scrumteam_plan(engine, plan_dict, team, plan['extra_opts']):
                    create_plan(engine, team, plan['name'], plan['type'], plan['extra_opts'])

    # mark users/teams/plans inactive
    if purge_old_users:
        logger.info('Users to mark inactive (%d)', len(users_to_mark_inactive))
        for username in users_to_mark_inactive:
            prune_target(engine, username, 'user')
        for team in teams_to_deactivate:
            prune_target(engine, team, 'team')

            for plan in get_teams_default_plans(config, team):
                plan_id = get_plan_id(engine, plan['name'])
                if plan_id:
                    deactivate_plan(engine, plan_id)

    session.commit()
    session.close()


def main():
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
