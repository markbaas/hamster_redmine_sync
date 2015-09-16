#!/usr/bin/env python3

import os
import os.path
import argparse
import configparser

from collections import defaultdict
from dateutil.relativedelta import relativedelta

import datetime
import logging
import re
import sys

from redmine import Redmine

from sqlalchemy import Column, Integer, String, ForeignKey, create_engine, DateTime
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


class Category(Base):
    __tablename__ = 'categories'

    id = Column(Integer, primary_key=True)
    name = Column(String)
    color_code = Column(String)
    category_order = Column(Integer)
    search_name = Column(String)

    activities = relationship('Activity', backref='category')


class Activity(Base):
    __tablename__ = 'activities'

    id = Column(Integer, primary_key=True)
    name = Column(String)
    work = Column(Integer)
    activity_order = Column(Integer)
    deleted = Column(Integer)
    category_id = Column(Integer, ForeignKey('categories.id'))
    search_name = Column(String)

    facts = relationship('Fact', backref='activity')
    facts_week = relationship(
        'Fact',
        primaryjoin='and_(Activity.id==Fact.activity_id, '
                    'Fact.start_time >= "{}", Fact.start_time <= "{}")'.format(*PERIODS['week']),
        order_by='Fact.start_time'
    )
    facts_month = relationship(
        'Fact',
        primaryjoin='and_(Activity.id==Fact.activity_id, '
                    'Fact.start_time >= "{}", Fact.start_time <= "{}")'.format(*PERIODS['month']),
        order_by='Fact.start_time'
    )

    def get_spent_time_per_day(self, begin_date, end_date, period='week'):
        d = defaultdict(float)
        for fact in getattr(self, 'facts_' + period):
            d[fact.start_time.date()] += fact.spent_time

        for key, raw_time in d.items():
            d[key] = round(raw_time * 2) / 2  # rounded on half hours

        return d


class Fact(Base):
    __tablename__ = 'facts'

    id = Column(Integer, primary_key=True)
    activity_id = Column(Integer, ForeignKey('activities.id'))
    start_time = Column(DateTime)
    end_time = Column(DateTime)
    description = Column(String)

    @hybrid_property
    def spent_time(self):
        if self.end_time is None:
            return -1
        else:
            # rounds on half hours
            return (self.end_time - self.start_time).seconds / 3600.


class HamsterRedmine:
    # Config
    db_path = os.path.join(os.path.expanduser('~'), '.local', 'share', 'hamster-applet',
                           'hamster.db')
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

    def _get_activities(self):
        return self.session.query(Activity)

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
        date_range = PERIODS.get(period)

        for activity in self._get_activities():
            try:
                issue_id = re.findall(r'#(\d+)', activity.name)[0]
            except IndexError:
                continue

            time_entries = activity.get_spent_time_per_day(*date_range).items()

            if not time_entries:
                continue

            print('\n{}'.format(activity.name))

            for date, spent_time in time_entries:
                if spent_time:
                    self._push_time_entry(spent_time, issue_id, date)


class Config(dict):
    def __init__(self):
        config = configparser.ConfigParser()
        config.read(os.path.join(os.path.expanduser('~'), '.config', 'hamster_redmine_sync.cfg'))
        config_values = config.items('general')
        for key, value in config_values:
            self[key] = value

        parser = argparse.ArgumentParser(description='hmaster to redmine sync')
        parser.add_argument('--redmine_user_id', type=int, help="redmine user id")
        parser.add_argument('--redmine_apikey', help="redmine apikey")
        parser.add_argument('--redmine_url', help="redmine url")
        parser.add_argument('--period', choices=PERIODS.keys(), help='sync period')

        args = vars(parser.parse_args())

        for key, value in args.items():
            if value:
                self[key] = value


if __name__ == '__main__':
    config = Config()
    sync = HamsterRedmine(**config)
    sync.sync_timeentries(period=config.get('period', 'week'))
