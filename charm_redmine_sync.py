#!/usr/bin/env python3

import os
import os.path
import argparse
import configparser

from collections import defaultdict
from dateutil.relativedelta import relativedelta

import datetime
import logging
import sys

from redmine import Redmine
from redmine.exceptions import ResourceAttrError

from sqlalchemy import Column, Integer, String, ForeignKey, create_engine
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.orm import relationship, sessionmaker
from sqlalchemy.ext.declarative import declarative_base


Base = declarative_base()

today = datetime.date.today()
PERIODS = {'month': (today.replace(day=1),
                     today.replace(day=1, month=today.month+1) - relativedelta(days=1)),
           'week': (today - relativedelta(days=today.weekday()),
                    today + relativedelta(days=6-today.weekday())),
           }


class Event(Base):
    __tablename__ = 'Events'

    id = Column(Integer, primary_key=True)
    user_id = Column(Integer)
    event_id = Column(Integer)
    installation_id = Column(Integer)
    report_id = Column(Integer)
    task = Column(Integer, ForeignKey('Tasks.task_id'))
    comment = Column(String)
    start = Column(String)
    end = Column(String)

    @hybrid_property
    def start_date(self):
        return datetime.datetime.strptime(self.start, '%Y-%m-%dT%H:%M:%S')

    @hybrid_property
    def end_date(self):
        return datetime.datetime.strptime(self.end, '%Y-%m-%dT%H:%M:%S')

    @hybrid_property
    def spent_time(self):
        if self.end_date is None:
            return (datetime.datetime.now() - self.start_date).seconds / 3600.
        else:
            return (self.end_date - self.start_date).seconds / 3600.


class Task(Base):
    __tablename__ = 'Tasks'

    id = Column(Integer, primary_key=True)
    task_id = Column(Integer)
    parent = Column(Integer)
    validfrom = Column(Integer)
    validuntil = Column(Integer)
    trackable = Column(Integer)
    name = Column(String)

    events = relationship('Event', backref='taska')
    events_week = relationship(
        'Event',
        primaryjoin='and_(Task.task_id==Event.task, '
                    'Event.start >= "{}", Event.start <= "{}")'.format(*PERIODS['week']),
        order_by='Event.start'
    )
    events_month = relationship(
        'Event',
        primaryjoin='and_(Task.task_id==Event.task, '
                    'Event.start >= "{}", Event.start <= "{}")'.format(*PERIODS['month']),
        order_by='Event.start'
    )

    def get_spent_time_per_day(self, period='week'):
        d = defaultdict(float)
        for event in getattr(self, 'events_' + period):
            d[event.start_date.date()] += event.spent_time

        for key, raw_time in d.items():
            d[key] = round(raw_time * 2) / 2  # rounded on half hours

        return d


class CharmRedmine:
    # Config
    db_path = os.path.join(os.path.expanduser('~'), '.local', 'share', 'data',
                           'KDAB', 'Charm', 'Charm.db')
    # db_path = os.path.join(os.getcwd(), 'hamster.db')
    redm = None
    session = None
    period = None
    redmine_user_id = None
    redmine_apikey = None
    redmine_url = None

    def __init__(self, **kwargs):
        for key, value in kwargs.items():
            setattr(self, key, value)

        self._create_db_session()
        self._connect_redmine()

    def _connect_redmine(self):
        key = self.redmine_apikey or os.environ['REDMINE_APIKEY']
        self.redm = Redmine(self.redmine_url, key=key)

    def _create_db_session(self):
        if not os.path.exists(self.db_path):
            logging.error('Database path is incorrect')
            sys.exit()

        engine = create_engine('sqlite:///' + self.db_path)
        self.session = sessionmaker(bind=engine)()

    def _get_tasks(self):
        return self.session.query(Task)

    def _push_time_entry(self, spent_time, issue_id, date):
        entries = self.redm.time_entry.filter(spent_on=date, issue_id=issue_id,
                                              user_id=self.redmine_user_id)

        if len(entries) > 1:
            logging.error('Sync does not support update multiple entries.')

        data = dict(spent_on=date, issue_id=issue_id, hours=spent_time, activity_id=9)

        if len(entries) == 1:
            if entries[0].hours != spent_time:
                self.redm.time_entry.update(entries[0].id, **data)
                print('  {}: {} hours (updated)'.format(date, spent_time))
        else:
            self.redm.time_entry.create(**data)
            print('  {}: {} hours (new)'.format(date, spent_time))

    def sync_timeentries(self, period='week'):
        for task in self._get_tasks():
            time_entries = task.get_spent_time_per_day(period=period).items()

            if not time_entries:
                continue

            print('#{} {}'.format(task.task_id, task.name))

            for date, spent_time in time_entries:
                if spent_time:
                    self._push_time_entry(spent_time, task.task_id, date)

            print('\n')

    def sync_redmine_issues(self):
        issues = self.redm.issue.filter(assigned_to_id='me')

        for issue in issues:
            task = self.session.query(Task)\
                .filter(Task.task_id == issue.id).first()
            if not task:
                try:
                    parent_id = issue.parent.id
                except ResourceAttrError:
                    parent_id = 0

                obj = Task(name=issue.subject, task_id=issue.id, trackable=1, parent=parent_id)
                self.session.add(obj)
                print('Added task {}'.format(issue.subject))

        # Check consistency
        for task in self.session.query(Task).all():
            if task.parent:
                parent = self.session.query(Task).filter(Task.task_id == task.parent).first()
                if not parent:
                    task.parent = 0
                self.session.add(task)

        self.session.commit()


class Config(dict):
    def __init__(self):
        config = configparser.ConfigParser()
        config.read(os.path.join(os.path.expanduser('~'), '.config', 'charm_redmine_sync.cfg'))
        config_values = config.items('general')
        for key, value in config_values:
            self[key] = value

        parser = argparse.ArgumentParser(description='hmaster to redmine sync')
        parser.add_argument('--redmine_user_id', type=int, help="redmine user id")
        parser.add_argument('--redmine_apikey', help="redmine apikey")
        parser.add_argument('--redmine_url', help="redmine url")
        parser.add_argument('--period', choices=PERIODS.keys(), help='sync period')
        parser.add_argument('--time_entries', type=int, choices=(1, 0),
                            help='push time entries TO redmine', default=1)
        parser.add_argument('--redmine_issues', type=int, choices=(1, 0),
                            help='pull issue FROM redmine', default=1)

        args = vars(parser.parse_args())

        for key, value in args.items():
            if value:
                self[key] = value


if __name__ == '__main__':
    config = Config()
    sync = CharmRedmine(**config)
    if config.get('time_entries'):
        print('''
Pushing time entries to redmine.
--------------------------------
''')
        sync.sync_timeentries(period=config.get('period', 'week'))
    if config.get('redmine_issues'):
        print('''
Pulling issues from redmine.
----------------------------
''')
        sync.sync_redmine_issues()
