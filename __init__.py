#!/usr/bin/env python3
# vim: set encoding=utf-8 tabstop=4 softtabstop=4 shiftwidth=4 expandtab
#########################################################################
#  Copyright 2022-         Michael Wenzel           wenzel_michael@web.de
#########################################################################
#  This file is part of SmartHomeNG.
#  https://www.smarthomeNG.de
#  https://knx-user-forum.de/forum/supportforen/smarthome-py
#
#  This plugin provides additional functionality to mysql database
#  connected via database plugin
#
#  SmartHomeNG is free software: you can redistribute it and/or modify
#  it under the terms of the GNU General Public License as published by
#  the Free Software Foundation, either version 3 of the License, or
#  (at your option) any later version.
#
#  SmartHomeNG is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU General Public License for more details.
#
#  You should have received a copy of the GNU General Public License
#  along with SmartHomeNG. If not, see <http://www.gnu.org/licenses/>.
#
#########################################################################

from lib.model.smartplugin import SmartPlugin
from lib.item import Items
from lib.item.item import Item
from lib.shtime import Shtime
from lib.plugin import Plugins
from .webif import WebInterface
import lib.db

import sqlvalidator
import datetime
from dateutil.relativedelta import *
from typing import Union
import time
import re

#########################################################################
# ToDo
#   - 'avg' for on-chance items implementieren
#   - on-change items last... implementieren
#   - zaehlerstand_tagesende implementieren
#########################################################################


class DatabaseAddOn(SmartPlugin):
    """
    Main class of the Plugin. Does all plugin specific stuff and provides the update functions for the items
    """

    std_req_dict = {
        'min_monthly_15m':           {'func': 'min',         'timespan': 'month', 'count': 15,   'group':     'month'},
        'max_monthly_15m':           {'func': 'max',         'timespan': 'month', 'count': 15,   'group':     'month'},
        'avg_monthly_15m':           {'func': 'avg',         'timespan': 'month', 'count': 15,   'group':     'month'},
        'min_weekly_30w':            {'func': 'min',         'timespan': 'week',  'count': 30,   'group':     'week'},
        'max_weekly_30w':            {'func': 'max',         'timespan': 'week',  'count': 30,   'group':     'week'},
        'avg_weekly_30w':            {'func': 'avg',         'timespan': 'week',  'count': 30,   'group':     'week'},
        'min_daily_30d':             {'func': 'min',         'timespan': 'day',   'count': 30,   'group':     'day'},
        'max_daily_30d':             {'func': 'max',         'timespan': 'day',   'count': 30,   'group':     'day'},
        'avg_daily_30d':             {'func': 'avg',         'timespan': 'day',   'count': 30,   'group':     'day'},
        'tagesmittelwert':           {'func': 'max',         'timespan': 'year',  'start': 0,    'end': 0,    'group': 'day'},
        'tagesmittelwert_hour':      {'func': 'avg1',        'timespan': 'day',   'start': 0,    'end': 0,    'group': 'hour', 'group2': 'day'},
        'tagesmittelwert_hour_days': {'func': 'avg1',        'timespan': 'day',   'count': 30,   'group':     'hour',  'group2': 'day'},
        'waermesumme_monthly_24m':   {'func': 'sum_max',     'timespan': 'month', 'start': 24,   'end': 0,    'group': 'day', 'group2': 'month'},
        'waermesumme_year_month':    {'func': 'sum_max',     'timespan': 'day',   'start': None, 'end': None, 'group': 'day', 'group2': None},
        'kaeltesumme_monthly_24m':   {'func': 'sum_max',     'timespan': 'month', 'start': 24,   'end': 0,    'group': 'day', 'group2': 'month'},
        'kaltesumme_year_month':     {'func': 'sum_min_neg', 'timespan': 'day',   'start': None, 'end': None, 'group': 'day', 'group2': None},
        'verbrauch_daily_30d':       {'func': 'diff_max',    'timespan': 'day',   'count': 30,   'group':     'day'},
        'verbrauch_week_30w':        {'func': 'diff_max',    'timespan': 'week',  'count': 30,   'group':     'week'},
        'verbrauch_month_18m':       {'func': 'diff_max',    'timespan': 'month', 'count': 18,   'group':     'month'},
        'zaehler_daily_30d':         {'func': 'max',         'timespan': 'day',   'count': 30,   'group':     'day'},
        'zaehler_week_30w':          {'func': 'max',         'timespan': 'week',  'count': 30,   'group':     'week'},
        'zaehler_month_18m':         {'func': 'max',         'timespan': 'month', 'count': 18,   'group':     'month'},
        'gts':                       {'func': 'max',         'timespan': 'year',  'start': None, 'end': None, 'group': 'day'},
        }

    PLUGIN_VERSION = '1.0.G'

    def __init__(self, sh):
        """
        Initializes the plugin.
        """

        # Call init code of parent class (SmartPlugin)
        super().__init__()

        # get item and shtime
        self.shtime = Shtime.get_instance()
        self.items = Items.get_instance()
        self.plugins = Plugins.get_instance()

        # define properties
        self._item_dict = {}                        # dict to hold all items {item1: ('_database_addon_fct', '_database_item'), item2: ('_database_addon_fct', '_database_item', _database_addon_params)...}
        self._daily_items = set()                   # set of items, for which the _database_addon_fct shall be executed daily
        self._weekly_items = set()                  # set of items, for which the _database_addon_fct shall be executed weekly
        self._monthly_items = set()                 # set of items, for which the _database_addon_fct shall be executed monthly
        self._yearly_items = set()                  # set of items, for which the _database_addon_fct shall be executed yearly
        self._onchange_items = set()                # set of items, for which the _database_addon_fct shall be executed if the database item has changed
        self._meter_items = set()                   # set of items, for which the _database_addon_fct shall be executed separately (create db entry short before midnight)
        self._startup_items = set()                 # set of items, for which the _database_addon_fct shall be executed on startup
        self._database_items = set()                # set of items with database attribute, relevant for plugin
        self._static_items = set()                  # set of items, for which the _database_addon_fct shall be executed just on startup
        self._itemid_dict = {}                      # dict to hold item_id for items
        self._oldest_log_dict = {}                  # dict to hold oldest_log for items
        self._oldest_entry_dict = {}                # dict to hold oldest_entry for items
        self.vortagsendwert_dict = {}               # dict to hold value of end of last day for items
        self.vorwochenendwert_dict = {}             # dict to hold value of end of last week for items
        self.vormonatsendwert_dict = {}             # dict to hold value of end of last month for items
        self.vorjahresendwert_dict = {}             # dict to hold value of end of last year for items
        self.tageswert_dict = {}                    # dict to hold min and max value of current day for items
        self.wochenwert_dict = {}                   # dict to hold min and max value of current week for items
        self.monatswert_dict = {}                   # dict to hold min and max value of current month for items
        self.jahreswert_dict = {}                   # dict to hold min and max value of current year for items
        self._webdata = {}                          # dict to hold information for webif update
        self._todo_items = set()                    # set of items, witch are due for calculation
        self._db_plugin = None                      # object if database plugin
        self._db = None                             # object of database
        self.connection_data = None                 # connection data list to database
        self.db_driver = None                       # driver for database
        self.last_connect_time = 0                  # mechanism for limiting db connection requests
        self.duration = 0                           # duration of a calculation cycle
        self.alive = None                           # Is plugin alive?
        self.activate_update = False                # Item updates for outside this plugin will be ignored until startup will be called
        self.execute_items_active = False           # Is there a running _execute_items method
        self.further_item_list = []                 # Buffer for item_list, used if _execute_items is still running
        self.parse_debug = True                     # Enable / Disable debug logging for method 'parse item'
        self.execute_debug = True                   # Enable / Disable debug logging for method 'execute items'
        self.sql_debug = True                       # Enable / Disable debug logging for sql stuff
        self.on_change_debug = True                 # Enable / Disable debug logging for method '_fill_cache_dicts'
        self.prepare_debug = True                   # Enable / Disable debug logging for query preparation
        self.default_connect_timeout = 60
        self.default_net_read_timeout = 60

        # get plugin parameters
        self.startup_run_delay = self.get_parameter_value('startup_run_delay')
        self.ignore_0_at_temp_items = self.get_parameter_value('ignore_0_at_temp_items')
        self.webif_pagelength = self.get_parameter_value('webif_pagelength')
        # self.db_instance = self.get_parameter_value('db_instance')

        # check existence of db-plugin, get parameters, and init connection to db
        if not self._check_db_existence():
            self.logger.error(f"Check of existence of database plugin incl connection check failed. Plugin not loaded")
            self._init_complete = False
        else:
            #  init connection to db
            self._db = lib.db.Database("DatabaseAddOn", self.db_driver, self.connection_data)
            if not self._db.api_initialized:
                self.logger.error("Initialization of database API failed")
                self._init_complete = False
            else:
                self.logger.debug("Initialization of database API successful")

        if not self._initialize_db():
            self._init_complete = False
            # pass

        self._check_db_connection_setting()

        # init webinterface
        if not self.init_webinterface(WebInterface):
            self.logger.error(f"Init of WebIF failed. Plugin not loaded")
            self._init_complete = False

    def run(self):
        """
        Run method for the plugin
        """

        self.logger.debug("Run method called")
        self.alive = True

        # add scheduler for cyclic trigger item calculation
        self.scheduler_add('cyclic', self.execute_due_items, prio=3, cron='5 0 0 * * *', cycle=None, value=None, offset=None, next=None)

        # add scheduler to trigger items to be calculated at startup with delay
        dt = self.shtime.now() + datetime.timedelta(seconds=(self.startup_run_delay + 3))
        self.scheduler_add('startup', self.execute_startup_items, next=dt)

    def stop(self):
        """
        Stop method for the plugin
        """

        self.logger.debug("Stop method called")
        self.scheduler_remove('cyclic')
        self.alive = False

    def parse_item(self, item):
        """
        Default plugin parse_item method. Is called when the plugin is initialized.

        The plugin can, corresponding to its attribute keywords, decide what to do with
        the item in future, like adding it to an internal array for future reference
        :param item:    The item to process.
        :return:        If the plugin needs to be informed of an items change you should return a call back function
                        like the function update_item down below. An example when this is needed is the knx plugin
                        where parse_item returns the update_item function when the attribute knx_send is found.
                        This means that when the items value is about to be updated, the call back function is called
                        with the item, caller, source and dest as arguments and in case of the knx plugin the value
                        can be sent to the knx with a knx write function within the knx plugin.
        """

        if self.has_iattr(item.conf, 'database_addon_fct'):
            if self.parse_debug:
                self.logger.debug(f"parse item: {item.id()} due to 'database_addon_fct'")

            # get attribute value
            _database_addon_fct = self.get_iattr_value(item.conf, 'database_addon_fct').lower()

            # get attribute if item should be calculated at plugin startup
            if self.has_iattr(item.conf, 'database_addon_startup'):
                _database_addon_startup = self.get_iattr_value(item.conf, 'database_addon_startup')
            else:
                _database_addon_startup = None

            # get attribute if certain value should be ignored at db query
            if self.has_iattr(item.conf, 'database_ignore_value'):
                _database_addon_ignore_value = self.get_iattr_value(item.conf, 'database_ignore_value')
            else:
                if self.ignore_0_at_temp_items and 'temp' in str(item.id()):
                    _database_addon_ignore_value = 0
                else:
                    _database_addon_ignore_value = None

            # get database item
            _database_item = None
            _lookup_item = item
            for i in range(3):
                if self.has_iattr(_lookup_item.conf, 'database'):
                    _database_item = _lookup_item
                    break
                else:
                    # self.logger.debug(f"Attribut 'database' is not found for item={item} at _lookup_item={_lookup_item}")
                    _lookup_item = _lookup_item.return_parent()

            # create items sets
            if _database_item is not None:
                # add item to item dict
                if self.parse_debug:
                    self.logger.debug(f"Item '{item.id()}' added with database_addon_fct={_database_addon_fct} and database_item={_database_item.id()}")
                self._item_dict[item] = (_database_addon_fct, _database_item, _database_addon_ignore_value)
                self._webdata.update({item.id(): {}})
                self._webdata[item.id()].update({'attribute': _database_addon_fct})

                # add item to be run on startup
                if _database_addon_startup is not None:
                    if self.parse_debug:
                        self.logger.debug(f"Item '{item.id()}' added to be run on startup")
                    self._startup_items.add(item)
                    self._webdata[item.id()].update({'startup': True})
                else:
                    self._webdata[item.id()].update({'startup': False})

                # handle items starting with 'zaehlerstand_tagesende'
                if _database_addon_fct == 'zaehlerstand_tagesende':
                    self._meter_items.add(item)
                    if self.parse_debug:
                        self.logger.debug(f"Function not implemented yet.")

                # handle items with 'heute_minus'
                elif 'heute_minus' in _database_addon_fct or _database_addon_fct.startswith('last'):
                    self._daily_items.add(item)
                    if self.parse_debug:
                        self.logger.debug(f"Item '{item.id()}' added to be run daily.")

                # handle items with 'woche_minus'
                elif 'woche_minus' in _database_addon_fct:
                    self._weekly_items.add(item)
                    if self.parse_debug:
                        self.logger.debug(f"Item '{item.id()}' added to be run weekly.")

                # handle items with 'monat_minus'
                elif 'monat_minus' in _database_addon_fct:
                    self._monthly_items.add(item)
                    if self.parse_debug:
                        self.logger.debug(f"Item '{item.id()}' added to be run monthly.")

                # handle items with 'jahr_minus'
                elif 'jahr_minus' in _database_addon_fct:
                    self._yearly_items.add(item)
                    if self.parse_debug:
                        self.logger.debug(f"Item '{item.id()}' added to be run yearly.")

                # handle static items starting with 'general_'
                elif _database_addon_fct.startswith('general_'):
                    self._static_items.add(item)
                    if self.parse_debug:
                        self.logger.debug(f"Item '{item.id()}' will not be cyclic.")

                # handle items with 'vorjahreszeitraum'
                elif 'vorjahreszeitraum' in _database_addon_fct:
                    self._daily_items.add(item)
                    if self.parse_debug:
                        self.logger.debug(f"Item '{item.id()}' added to be run daily.")

                # handle all functions with 'summe' like waermesumme, kaeltesumme, gruenlandtemperatursumme
                elif 'summe' in _database_addon_fct:
                    if self.db_driver.lower() != 'pymysql':
                        self.logger.warning(f"Functionality of '_database_addon_fct' not given with type of connected database. Item will be ignored.")
                    else:
                        if self.has_iattr(item.conf, 'database_addon_params'):
                            _database_addon_params = params_to_dict(self.get_iattr_value(item.conf, 'database_addon_params'))
                            if _database_addon_params is None:
                                self.logger.warning(f"Error occurred during parsing of item attribute 'database_addon_params' of item {item.id()}. Item will be ignored.")
                            else:
                                if 'year' in _database_addon_params:
                                    _database_addon_params['item'] = _database_item
                                    self._item_dict[item] = self._item_dict[item] + (_database_addon_params,)
                                    self._daily_items.add(item)
                                    if self.parse_debug:
                                        self.logger.debug(f"Item '{item.id()}' added to be run daily.")
                                else:
                                    self.logger.warning(f"Item '{item.id()}' with database_addon_fct={_database_addon_fct} ignored, since parameter 'year' not given in database_addon_params={_database_addon_params}. Item will  be ignored")
                        else:
                            self.logger.warning(f"Item '{item.id()}' with database_addon_fct={_database_addon_fct} ignored, since parameter using 'database_addon_params' not given. Item will be ignored.")

                # handle tagesmitteltemperatur
                elif _database_addon_fct == 'tagesmitteltemperatur':
                    if self.db_driver.lower() != 'pymysql':
                        self.logger.warning(f"Functionality of '_database_addon_fct' not given with type of connected database. Item will be ignored.")
                    else:
                        if self.has_iattr(item.conf, 'database_addon_params'):
                            _database_addon_params = params_to_dict(self.get_iattr_value(item.conf, 'database_addon_params'))
                            _database_addon_params['item'] = _database_item
                            self._item_dict[item] = self._item_dict[item] + (_database_addon_params,)
                            self._daily_items.add(item)
                        else:
                            self.logger.warning(f"Item '{item.id()}' with database_addon_fct={_database_addon_fct} ignored, since parameter using 'database_addon_params' not given. Item will be ignored.")

                # handle db_request
                elif _database_addon_fct == 'db_request':
                    if self.db_driver.lower() != 'pymysql':
                        self.logger.warning(f"Functionality of '_database_addon_fct' not given with type of connected database. Item will be ignored.")
                    else:
                        if self.has_iattr(item.conf, 'database_addon_params'):
                            _database_addon_params = self.get_iattr_value(item.conf, 'database_addon_params')
                            if _database_addon_params in self.std_req_dict:
                                _database_addon_params = self.std_req_dict[_database_addon_params]
                            elif '=' in _database_addon_params:
                                _database_addon_params = params_to_dict(_database_addon_params)
                            if _database_addon_params is None:
                                self.logger.warning(f"Error occurred during parsing of item attribute 'database_addon_params' of item {item.id()}. Item will be ignored.")
                            else:
                                if self.parse_debug:
                                    self.logger.debug(f"parse_item: _database_addon_fct={_database_addon_fct} for item={item.id()}, _database_addon_params={_database_addon_params}")
                                if any(k in _database_addon_params for k in ('func', 'timespan')):
                                    _database_addon_params['item'] = _database_item
                                    self._item_dict[item] = self._item_dict[item] + (_database_addon_params,)
                                    _timespan = _database_addon_params.get('group', None)
                                    if not _timespan:
                                        _timespan = _database_addon_params.get('timespan', None)
                                    if _timespan == 'day':
                                        self._daily_items.add(item)
                                        if self.parse_debug:
                                            self.logger.debug(f"Item '{item.id()}' added to be run daily.")
                                    elif _timespan == 'week':
                                        self._weekly_items.add(item)
                                        if self.parse_debug:
                                            self.logger.debug(f"Item '{item.id()}' added to be run weekly.")
                                    elif _timespan == 'month':
                                        self._monthly_items.add(item)
                                        if self.parse_debug:
                                            self.logger.debug(f"Item '{item.id()}' added to be run monthly.")
                                    elif _timespan == 'year':
                                        self._yearly_items.add(item)
                                        if self.parse_debug:
                                            self.logger.debug(f"Item '{item.id()}' added to be run yearly.")
                                    else:
                                        self.logger.warning(f"Item '{item.id()}' with database_addon_fct={_database_addon_fct} ignored. Not able to detect update cycle.")
                                else:
                                    self.logger.warning(f"Item '{item.id()}' with database_addon_fct={_database_addon_fct} ignored, not all mandatory parameters in database_addon_params={_database_addon_params} given. Item will be ignored.")
                        else:
                            self.logger.warning(f"Item '{item.id()}' with database_addon_fct={_database_addon_fct} ignored, since parameter using 'database_addon_params' not given. Item will be ignored")

                # handle on_change items
                else:
                    self._onchange_items.add(item)
                    if self.parse_debug:
                        self.logger.debug(f"Item '{item.id()}' added to be run on-change.")
                    self._database_items.add(_database_item)

                # create data for webIF
                _update_cycle = 'None'
                if item in self._daily_items:
                    _update_cycle = 't??glich'
                elif item in self._weekly_items:
                    _update_cycle = 'w??chentlich'
                elif item in self._monthly_items:
                    _update_cycle = 'monatlich'
                elif item in self._yearly_items:
                    _update_cycle = 'j??hrlich'
                elif item in self._onchange_items:
                    _update_cycle = 'on-change'
                self._webdata[item.id()].update({'cycle': _update_cycle})

        # Callback mit 'update_item' f??r alle Items mit Attribut 'database', um die on_change Items zu berechnen
        if self.has_iattr(item.conf, 'database'):
            return self.update_item

    def update_item(self, item, caller=None, source=None, dest=None):
        """
        Handle updated item

        This method is called, if the value of an item has been updated by SmartHomeNG.
        It should write the changed value out to the device (hardware/interface) that
        is managed by this plugin.

        :param item: item to be updated towards the plugin
        :param caller: if given it represents the callers name
        :param source: if given it represents the source
        :param dest: if given it represents the dest
        """

        if self.alive and caller != self.get_shortname() and item in self._database_items:
            self.logger.debug(f"update_item was called with item {item.property.path} with value {item()} from caller {caller}, source {source} and dest {dest}")
            if not self.activate_update:
                self.logger.debug(f"update_item: Update method is paused for startup.")
            else:
                self._fill_cache_dicts(item, item())

    def execute_due_items(self) -> None:
        """
        Execute all due_items
        """

        if self.execute_debug:
            self.logger.debug("execute_due_items called")

        _todo_items = list(self._create_due_items())
        self.logger.info(f"execute_due_items: Following {len(_todo_items)} items will be calculated: {_todo_items}")

        self.execute_items(_todo_items)

    def execute_startup_items(self) -> None:
        """
        Execute all startup_items
        """
        if self.execute_debug:
            self.logger.debug("execute_startup_items called")

        self.activate_update = True
        self.execute_items(list(self._startup_items))

    def execute_all_items(self) -> None:
        """
        Execute all items
        """

        self.logger.info(f"All item will be calculated!! That will be {len(list(self._item_dict.keys()))} items!")
        self.execute_items(list(self._item_dict.keys()))

    def execute_items(self, item_list: list) -> None:
        """
        Execute functions per item based on given item list

        :param item_list: list of items to be executed
        :type item_list: list
        """

        # check if another cyclic cmd run is still active and handle it
        if self.execute_items_active:
            self.logger.warning('Execute_items called, but previous run is still active. Run will be started after current one.')
            self.further_item_list.extend(item_list)
            return

        # set lock
        self.execute_items_active = True

        _item_count = len(item_list)

        self.logger.info(f"execute_items: START calculating {_item_count} items. item_list={item_list}")
        _start_time = time.time()

        for index, item in enumerate(item_list):
            _database_addon_fct = self._item_dict[item][0]
            _database_addon_params = None
            _database_item = self._item_dict[item][1]
            _var = _database_addon_fct.split('_')
            _ignore_value = self._item_dict[item][2]
            _time_str_1 = None
            _time_str_2 = None
            _result = None

            self.logger.info(f"Item No. {index+1}/{_item_count} '{item.id()}' will be processed with _database_addon_fct={_database_addon_fct} _database_item={_database_item.id()}")

            # handle general functions
            if _database_addon_fct.startswith('general_'):
                # handle oldest_value
                if _database_addon_fct == 'general_oldest_value':
                    _result = self._get_oldest_value(_database_item)

                # handle oldest_log
                elif _database_addon_fct == 'general_oldest_log':
                    _result = self._get_oldest_log(_database_item)

                # handle db_version
                elif _database_addon_fct == 'general_db_version':
                    _result = self._get_db_version()

            # handle item starting with 'verbrauch_'
            elif _database_addon_fct.startswith('verbrauch_'):

                # handle all on_change functions of format 'verbrauch_timeframe' like 'verbrauch_heute'
                if len(_var) == 2 and _var[1] in ['heute', 'woche', 'monat', 'jahr']:
                    if self.execute_debug:
                        self.logger.debug(f"on_change function={_var[1]} detected; will be calculated by next change of database item")

                # handle all functions 'verbrauch' in format 'verbrauch_timeframe_timedelta' like 'verbrauch_heute_minus2'
                elif len(_var) == 3 and _var[1] in ['heute', 'woche', 'monat', 'jahr'] and _var[2].startswith('minus'):
                    _timeframe = _var[1]
                    _timedelta = _var[2][-1]

                    if self.execute_debug:
                        self.logger.debug(f"execute_items: '{_database_addon_fct}' function detected. _timeframe={_timeframe}, _timedelta={_timedelta}")

                    if isinstance(_timedelta, str) and _timedelta.isdigit():
                        _timedelta = int(_timedelta)

                    if isinstance(_timedelta, int):
                        _result = self._query_item('max1', _database_item, _timeframe, start=_timedelta+1, end=_timedelta, group=_timeframe, group2=_timeframe)

                # handle all functions of format 'verbrauch_function_window_timeframe_timedelta' like 'verbrauch_rolling_12m_woche_minus1'
                elif len(_var) == 5 and _var[1] == 'rolling' and _var[4].startswith('minus'):
                    _func = _var[1]
                    _window = _var[2]
                    _window_inc = int(_window[:-1])
                    _window_dur = _window[-1]
                    _timeframe = _var[3]
                    _timedelta = _var[4][-1]

                    # time conversion
                    _d_in_y = 365
                    _d_in_w = 7
                    _m_in_y = 12
                    _w_in_y = _d_in_y / _d_in_w
                    _w_in_m = _w_in_y / _m_in_y
                    _d_in_m = _d_in_y / _m_in_y

                    conversion = {
                        'heute': {'d': 1,
                                  'w': _d_in_w,
                                  'm': _d_in_m,
                                  'y': _d_in_y,
                                  },
                        'woche': {'d': 1 / _d_in_w,
                                  'w': 1,
                                  'm': _w_in_m,
                                  'y': _w_in_y
                                  },
                        'monat': {'d': 1 / _d_in_m,
                                  'w': 1 / _w_in_m,
                                  'm': 1,
                                  'y': _m_in_y
                                  },
                        'jahr': {'d': 1 / _d_in_y,
                                 'w': 1 / _w_in_y,
                                 'm': 1 / _m_in_y,
                                 'y': 1
                                 }
                        }

                    if self.execute_debug:
                        self.logger.debug(f"execute_items: {_func} function detected. _window={_window}, _timeframe={_timeframe}, _timedelta={_timedelta}")

                    if isinstance(_timedelta, str) and _timedelta.isdigit():
                        _timedelta = int(_timedelta)
                        _endtime = _timedelta

                        if _func == 'rolling' and _window_dur in ['d', 'w', 'm', 'y']:
                            _starttime = int(round(conversion[_timeframe][_window_dur] * _window_inc, 0))
                            _result = self._query_item('max1', _database_item, _timeframe, start=_starttime, end=0, group=_timeframe, group2=_timeframe)

                # handle all functions of format 'verbrauch_timeframe_timedelta' like 'verbrauch_vorjahreszeitraum_minus0'
                elif len(_var) == 3 and _var[1] == 'vorjahreszeitraum' and _var[2].startswith('minus'):
                    _timeframe = _var[1]
                    _timedelta = _var[2][-1]

                    if self.execute_debug:
                        self.logger.debug(f"execute_items: '{_database_addon_fct}' function detected. _timeframe={_timeframe}, _timedelta={_timedelta}")

                    if isinstance(_timedelta, str) and _timedelta.isdigit():
                        _timedelta = int(_timedelta)

                    if isinstance(_timedelta, int):
                        _today = datetime.date.today()
                        _year = _today.year - 1 - _timedelta
                        # start ist 1.1. des Vorjahrs minus _timedelta
                        _start_date = datetime.date(int(_year), 1, 1)
                        # ende ist heute des Vorjahres minus _timedelta
                        _end_date = _today + relativedelta(years=- 1 - _timedelta)
                        _start = (_today - _start_date).days
                        _end = (_today - _end_date).days

                        _result = self._query_item('max1', _database_item, _timeframe, start=_start, end=_end, group='year', group2='year')
                        
                if _result and _result < 0:
                    self.logger.warning(f"Result of item {item.id()} with _database_addon_fct={_database_addon_fct} was negativ. Something seems to be wrong.")

            # handle item starting with 'zaehlerstand_' of format 'zaehlerstand_timeframe_timedelta' like 'zaehlerstand_woche_minus1'
            elif _database_addon_fct.startswith('zaehlerstand_') and len(_var) == 3 and _var[2].startswith('minus'):

                _func = _var[0]
                _timeframe = _var[1]
                _timedelta = _var[2][-1]

                if self.execute_debug:
                    self.logger.debug(f"execute_items: {_func} function detected. _timeframe={_timeframe}, _timedelta={_timedelta}")

                if isinstance(_timedelta, str) and _timedelta.isdigit():
                    _timedelta = int(_timedelta)

                if _func == 'zaehlerstand':
                    _result = self._query_item('max', _database_item, _timeframe, start=_timedelta, end=_timedelta)
                    if self.execute_debug:
                        self.logger.debug(f"zaehlerstand: _result={_result}")
                    if _result == 0:
                        if self.execute_debug:
                            self.logger.debug(f"execute_items: _result={_result}. It seem that there was no entry for _timeframe={_timeframe}, _timedelta={_timedelta} in DB. Try to get last value prior to that.")
                        _result = self._query_item('max', _database_item, _timeframe, start=_timedelta+10, end=_timedelta+1)
                        if self.execute_debug:
                            self.logger.debug(f"execute_items: Next valid entry found is {_result}.")

            # handle item starting with 'minmax_'
            elif _database_addon_fct.startswith('minmax_'):

                # handle all on_change functions of format 'minmax_timeframe_function' like 'minmax_heute_max'
                if len(_var) == 3 and _var[1] in ['heute', 'woche', 'monat', 'jahr'] and _var[2] in ['min', 'max']:
                    self.logger.info(f"on_change function={_var[0]} with {_var[1]} detected; will be calculated by next change of database item")

                # handle all 'last' functions in format 'minmax_last_window_function' like 'minmax_last_24h_max'
                elif len(_var) == 4 and _var[1] == 'last':
                    _window = _var[2]
                    _func = _var[3]
                    _timeframe = _window[-1:]
                    _timedelta = _window[:-1]

                    if self.execute_debug:
                        self.logger.debug(f"execute_items: 'last' function detected. _window={_window}, _func={_func}")

                    if _timeframe in ['d', 'w', 'm', 'y'] and _timedelta.isdigit():
                        _result = self._query_item(_func, _database_item, _timeframe, start=_timedelta, end=0, ignore_value=_ignore_value)

                # handle all functions 'min/max/avg' in format 'minmax_timeframe_timedelta_func' like 'minmax_heute_minus2_max'
                elif len(_var) == 4 and _var[3] in ['min', 'max', 'avg']:
                    _timeframe = _var[1]            # heute, woche, monat
                    _timedelta = _var[2][-1]        # 1, 2, 3, ...
                    _func = _var[3]                 # min, max, avg

                    if self.execute_debug:
                        self.logger.debug(f"execute_items: _database_addon_fct={_func} detected; _timeframe={_timeframe}, _timedelta={_timedelta}")

                    if isinstance(_timedelta, str) and _timedelta.isdigit():
                        _timedelta = int(_timedelta)

                    if isinstance(_timedelta, int):
                        _result = self._query_item(_func, _database_item, _timeframe, start=_timedelta+1, end=_timedelta, ignore_value=_ignore_value)

            # handle kaeltesumme, waermesumme, gruendlandtempsumme
            elif 'summe' in _database_addon_fct:
                _database_addon_params = self._item_dict[item][3]

                if self.execute_debug:
                    self.logger.debug(f"execute_items: _database_addon_fct={_database_addon_fct} detected; _database_addon_params={_database_addon_params}")

                if _database_addon_params.keys() & {'item', 'year'}:
                    if _database_addon_fct == 'kaeltesumme':
                        _result = self.kaeltesumme(**_database_addon_params)
                    elif _database_addon_fct == 'waermesumme':
                        _result = self.waermesumme(**_database_addon_params)
                    elif _database_addon_fct == 'gruendlandtempsumme':
                        _result = self.gruenlandtemperatursumme(**_database_addon_params)
                else:
                    self.logger.warning(f"Attribute 'database_addon_params' for item {item.id()} not containing needed params with _database_addon_fct={_database_addon_fct}.")

            # handle tagesmitteltemperatur
            elif _database_addon_fct == 'tagesmitteltemperatur':
                _database_addon_params = self._item_dict[item][3]

                if self.execute_debug:
                    self.logger.debug(f"execute_items: _database_addon_fct={_database_addon_fct} detected; _database_addon_params={_database_addon_params}")

                if _database_addon_params.keys() & {'item'}:
                    _result = self.tagesmitteltemperatur(**_database_addon_params)

            # handle db_request
            elif _database_addon_fct == 'db_request':
                _database_addon_params = self._item_dict[item][3]

                if self.execute_debug:
                    self.logger.debug(f"execute_items: _database_addon_fct={_database_addon_fct} detected; _database_addon_params={_database_addon_params}")

                if _database_addon_params.keys() & {'func', 'item', 'timespan'}:
                    _result = self.fetch_log(**_database_addon_params)
                else:
                    self.logger.error(f"Attribute 'database_addon_params' not containing needed params for Item {item.id} with _database_addon_fct={_database_addon_fct}.")

            # handle everything else
            else:
                self.logger.warning(f"execute_items: Function {_database_addon_fct } for item {item.id()} not defined or found")

            # log result
            if self.execute_debug:
                self.logger.debug(f"execute_items: result is {_result} for item '{item.id()}' with _database_addon_fct={_database_addon_fct} _database_item={_database_item.id()} _database_addon_params={_database_addon_params}")

            # set item value and put data into webif update dict
            if _result is not None:
                if self.execute_debug:
                    self.logger.debug(f"execute_items: Item value of item '{item.id()}' will be set to {_result}")
                self._webdata[item.id()].update({'value': _result})
                item(_result, self.get_shortname())

        self.duration = int(time.time() - _start_time)
        self.logger.info(f"execute_items: FINISHED calculating {len(item_list)} items within {self.duration} sec.")

        # release lock
        self.execute_items_active = False

        if self.further_item_list:
            if self.execute_debug:
                self.logger.debug(f"Execute_items now runs items called during last run.")
            new_item_list = list(set(self.further_item_list))
            self.further_item_list = []
            self.execute_items(new_item_list)

    @property
    def get_log_level(self):
        return self.logger.getEffectiveLevel()

    ##############################
    #       Public functions
    ##############################

    def gruenlandtemperatursumme(self, item, year: Union[int, str]) -> Union[int, None]:
        """
        Query database for gruenlandtemperatursumme for given year or year/month

        :param item: item object or item_id for which the query should be done
        :type item: item
        :param year: year the gruenlandtemperatursumme should be calculated for
        :type year: str

        :return: gruenlandtemperatursumme
        :rtype: int
        """

        if not valid_year(year):
            self.logger.error(f"gruenlandtemperatursumme: Year for item={item.id()} was {year}. This is not a valid year. Query cancelled.")
            return

        current_year = datetime.date.today().year

        if year == 'current':
            year = current_year

        year = int(year)
        year_delta = current_year - year
        if year_delta < 0:
            self.logger.error(f"gruenlandtemperatursumme: Start time for query is in future. Query cancelled.")
            return

        _database_addon_params = self.std_req_dict.get('gts', None)
        _database_addon_params['start'] = year_delta
        _database_addon_params['end'] = year_delta
        _database_addon_params['item'] = item

        result = self.fetch_log(**_database_addon_params)

        gts = 0
        for entry in result:
            dt = datetime.datetime.fromtimestamp(int(entry[0]) / 1000)
            if dt.month == 1:
                gts += float(entry[1]) * 0.5
            elif dt.month == 2:
                gts += float(entry[1]) * 0.75
            else:
                gts += entry[1]
        return int(round(gts, 0))

    def waermesumme(self, item, year, month: Union[int, str] = None) -> Union[int, None]:
        """
        Query database for waermesumme for given year or year/month

        :param item: item object or item_id for which the query should be done
        :type item: item
        :param year: year the waermesumme should be calculated for
        :type year: str
        :param month: month the waermesumme should be calculated for
        :type month: str

        :return: waermesumme
        :rtype: int
        """

        if not valid_year(year):
            self.logger.error(f"waermesumme: Year for item={item.id()} was {year}. This is not a valid year. Query cancelled.")
            return

        if year == 'current':
            year = datetime.date.today().year

        if month is None:
            start_date = datetime.date(int(year), 3, 20)
            end_date = datetime.date(int(year), 9, 21)
            group2 = 'year'
        elif valid_month(month):
            start_date = datetime.date(int(year), int(month), 1)
            end_date = start_date + relativedelta(months=+1) - datetime.timedelta(days=1)
            group2 = 'month'
        else:
            self.logger.error(f"waermesumme: Month for item={item.id()} was {month}. This is not a valid month. Query cancelled.")
            return

        today = datetime.date.today()
        if start_date > today:
            self.logger.info(f"waermesumme: Start time for query is in future. Query cancelled.")
            return

        start = (today - start_date).days
        end = (today - end_date).days if end_date < today else 0
        if start < end:
            self.logger.error(f"waermesumme: End time for query is before start time. Query cancelled.")
            return

        _database_addon_params = self.std_req_dict.get('waermesumme_year_month', None)
        _database_addon_params['start'] = start
        _database_addon_params['end'] = end
        _database_addon_params['group2'] = group2
        _database_addon_params['item'] = item

        result = self.fetch_log(**_database_addon_params)
        self.logger.debug(f"waermesumme_year_month: {result=} for {item.id()=} with {year=} and {month=}")

        if result:
            return int(result[0][1])

    def kaeltesumme(self, item, year, month: Union[int, str] = None) -> Union[int, None]:
        """
        Query database for kaeltesumme for given year or year/month

        :param item: item object or item_id for which the query should be done
        :type item: item
        :param year: year the kaeltesumme should be calculated for
        :type year: str
        :param month: month the kaeltesumme should be calculated for
        :type month: str

        :return: kaeltesumme
        :rtype: int
        """

        if not valid_year(year):
            self.logger.error(f"kaeltesumme: Year for item={item.id()} was {year}. This is not a valid year. Query cancelled.")
            return

        if year == 'current':
            if datetime.date.today() < datetime.date(int(datetime.date.today().year), 9, 21):
                year = datetime.date.today().year - 1
            else:
                year = datetime.date.today().year

        if month is None:
            start_date = datetime.date(int(year), 9, 21)
            end_date = datetime.date(int(year) + 1, 3, 22)
            group2 = 'year'
        elif valid_month(month):
            start_date = datetime.date(int(year), int(month), 1)
            end_date = start_date + relativedelta(months=+1) - datetime.timedelta(days=1)
            group2 = 'month'
        else:
            self.logger.error(f"kaeltesumme: Month for item={item.id()} was {month}. This is not a valid month. Query cancelled.")
            return

        today = datetime.date.today()
        if start_date > today:
            self.logger.error(f"kaeltesumme: Start time for query is in future. Query cancelled.")
            return

        start = (today - start_date).days
        end = (today - end_date).days if end_date < today else 0
        if start < end:
            self.logger.error(f"kaeltesumme: End time for query is before start time. Query cancelled.")
            return

        _database_addon_params = self.std_req_dict.get('kaltesumme_year_month', None)
        _database_addon_params['start'] = start
        _database_addon_params['end'] = end
        _database_addon_params['group2'] = group2
        _database_addon_params['item'] = item

        result = self.fetch_log(**_database_addon_params)
        self.logger.debug(f"kaeltesumme: {result=} for {item.id()=} with {year=} and {month=}")

        value = 0
        if result:
            if month:
                value = result[0][1]
            else:
                for entry in result:
                    value += entry[1]
            return int(value)

    def tagesmitteltemperatur(self, item, count: int = None) -> list:
        """
        Query database for tagesmitteltemperatur

        :param item: item object or item_id for which the query should be done
        :type item: item
        :param count: start of timeframe defined by number of time increments starting from now to the left (into the past)
        :type count: int

        :return: tagesmitteltemperatur
        :rtype: list of tuples
        """

        _database_addon_params = self.std_req_dict.get('tagesmittelwert_hour_days', None)
        if count:
            _database_addon_params['count'] = count
        _database_addon_params['item'] = item

        return self.fetch_log(**_database_addon_params)

    def fetch_log(self, func: str, item, timespan: str, start: int = None, end: int = 0, count: int = None, group: str = None, group2: str = None, ignore_value=None) -> Union[list, None]:
        """
        Query database, format response and return it

        :param func: function to be used at query
        :type func: str
        :param item: item object or item_id for which the query should be done
        :type item: item
        :param timespan: time increment f??r definition of start, end, count (day, week, month, year)
        :type timespan: str
        :param start: start of timeframe (oldest) for query given in x time increments (default = None, meaning complete database)
        :type start: int
        :param end: end of timeframe (newest) for query given in x time increments (default = 0, meaning today, end of last week, end of last month, end of last year)
        :type end: int
        :param count: start of timeframe defined by number of time increments starting from end to the left (into the past)
        :type count: int
        :param group: first grouping parameter (default = None, possible values: day, week, month, year)
        :type group: str
        :param group2: second grouping parameter (default = None, possible values: day, week, month, year)
        :type group2: str
        :param ignore_value: value of val_num, which will be ignored during query
        :type ignore_value: num

        :return: formated query response
        :rtype: list
        """

        # query
        query_result = self._query_log(func, item, timespan, start, end, count, group, group2, ignore_value)

        # handle result
        value = []
        for element in query_result:
            value.append([element[0], element[1]])
        if func == 'diff_max':
            value.pop(0)
        self.logger.debug(f"fetch_log: value for item={item.id()} with timespan={timespan}, func={func}: {value}")
        return value

    def fetch_raw(self, query: str, params: dict = None) -> Union[list, None]:
        """
        Fetch database with given query string and params

        :param query: database query to be executed
        :type query: str
        :param params: query parameters
        :type params: dict

        :return: result of database query
        :rtype: tuples
        """

        if params is None:
            params = {}

        formatted_sql = sqlvalidator.format_sql(query)
        sql_query = sqlvalidator.parse(formatted_sql)

        if not sql_query.is_valid():
            self.logger.error(f"fetch_log_raw: Validation of query failed with error: {sql_query.errors}")
            return

        # return request of database
        return self._fetchall(query, params)

    @property
    def item_list(self):
        return list(self._item_dict.keys())

    ##############################
    #        Support stuff
    ##############################

    def _check_db_connection_setting(self) -> None:
        connect_timeout = int(self._get_db_connect_timeout()[1])
        if connect_timeout != self.default_connect_timeout:
            self.logger.warning(f"DB variable 'connect_timeout' should be adjusted for proper working to {self.default_connect_timeout}. Current setting is {connect_timeout}. You need to insert adequate entries into /etc/mysql/my.cnf within section [mysqld].")

        net_read_timeout = int(self._get_db_net_read_timeout()[1])
        if net_read_timeout != self.default_net_read_timeout:
            self.logger.warning(f"DB variable 'net_read_timeout' should be adjusted for proper working to {self.default_net_read_timeout}. Current setting is {net_read_timeout}. You need to insert adequate entries into /etc/mysql/my.cnf within section [mysqld].")

    def _fill_cache_dicts(self, updated_item, value) -> None:
        """
        Get item and item value for which an update has been detected, fill cache dicts and set item value.

        :param updated_item: Item which has been updated
        :type updated_item: item
        :param value: Value of updated item
        :type value: foo
        """

        if self.on_change_debug:
            self.logger.debug(f"_fill_cache_dicts called with updated_item={updated_item.id()} and value={value}.")

        map_dict = {
            'heute': self.tageswert_dict,
            'woche': self.wochenwert_dict,
            'monat': self.monatswert_dict,
            'jahr': self.jahreswert_dict
            }

        map_dict1 = {
            'heute': self.vortagsendwert_dict,
            'woche': self.vorwochenendwert_dict,
            'monat': self.vormonatsendwert_dict,
            'jahr': self.vorjahresendwert_dict
            }

        for item in self._onchange_items:
            _database_item = self._item_dict[item][1]
            if value and _database_item == updated_item:
                _database_addon_fct = self._item_dict[item][0]
                _var = _database_addon_fct.split('_')
                _ignore_value = self._item_dict[item][2]

                # handle minmax on-change items like minmax_heute_max, minmax_heute_min, minmax_woche_max, minmax_woche_min.....
                if _database_addon_fct.startswith('minmax') and len(_var) == 3 and _var[2] in ['min', 'max']:
                    _timeframe = _var[1]
                    _func = _var[2]
                    _cache_dict = map_dict[_timeframe]

                    if self.on_change_debug:
                        self.logger.debug(f"_fill_cache_dicts: minmax item {updated_item.id()} detected. Check for update of _cache_dicts and item value.")

                    # check if database item is in cache dict; if not add it
                    if _database_item not in _cache_dict:
                        _cache_dict[_database_item] = {}
                    if _cache_dict[_database_item].get(_func, None) is None:
                        _cache_dict[_database_item][_func] = self._query_item(_func, _database_item, _timeframe, start=1, end=0, ignore_value=_ignore_value)
                    if self.on_change_debug:
                        self.logger.debug(f"_fill_cache_dicts: Item={updated_item.id()} with _func={_func} and _timeframe={_timeframe} not in cache dict. Value {_cache_dict[_database_item][_func]} has been added.")

                    # update cache dicts
                    _update = False
                    _cached_value = _cache_dict[_database_item][_func]
                    if _cached_value:
                        if _func == 'min' and value < _cached_value:
                            _update = True
                            if self.on_change_debug:
                                self.logger.debug(f"_fill_cache_dicts: new value={value} lower then current min_value={_cache_dict[_database_item][_func]}. _cache_dict will be updated")
                        elif _func == 'max' and value > _cached_value:
                            _update = True
                            if self.on_change_debug:
                                self.logger.debug(f"_fill_cache_dicts: new value={value} higher then current max_value={_cache_dict[_database_item][_func]}. _cache_dict will be updated")
                        if _update:
                            _cache_dict[_database_item][_func] = value

                        # set item value and put data into webif update dict
                        value = _cached_value
                        if self.on_change_debug:
                            self.logger.debug(f"_fill_cache_dicts: on-change item={item.id()} with func={_func} will be set to {value}; current item value={item()}.")
                        self._webdata[item.id()].update({'value': value})
                        item(value, self.get_shortname())

                # handle verbrauch und zaehlerstand on-change items ending with heute, woche, monat, jahr
                elif _database_addon_fct.startswith('verbrauch') and len(_var) == 2 and _var[1] in ['heute', 'woche', 'monat', 'jahr']:
                    _timeframe = _var[1]
                    _cache_dict = map_dict1[_timeframe]

                    if self.on_change_debug:
                        self.logger.debug(f"_fill_cache_dicts: verbrauch item {updated_item.id()} detected. Check for update of _cache_dicts and item value.")

                    # update cache dicts
                    if _database_item not in _cache_dict:
                        value = self._query_item('max', _database_item, _timeframe, start=1, end=0)
                        _cache_dict[_database_item] = value
                        if self.on_change_debug:
                            self.logger.debug(f"_fill_cache_dicts: Item={updated_item.id()} with _timeframe={_timeframe} not in cache dict. Value {value} has been added.")

                    # calculate value
                    _cached_value = _cache_dict[_database_item]
                    if _cached_value:
                        delta_value = round(value - _cached_value, 1)

                        # set item value
                        if self.on_change_debug:
                            self.logger.debug(f"_fill_cache_dicts: on-change item={item.id()} will be set to value={delta_value}; current item value={item()}.")
                        item(delta_value, self.get_shortname())

    def _get_itemid(self, item) -> int:
        """
        Returns the ID of the given item from cache dict or request it from database

        :param item: Item to get the ID for
        :type item: item

        :return: id of the item within the database
        :rtype: int | None
        """

        self.logger.debug(f"_get_itemid called with item={item.id()}")

        _item_id = self._itemid_dict.get(item, None)
        if _item_id is None:
            row = self._read_item_table(item)
            if row:
                if len(row) > 0:
                    _item_id = int(row[0])
                    self._itemid_dict[item] = _item_id
        return _item_id

    def _create_due_items(self) -> set:
        """
        Create list of items which are due, resets cache dicts

        :return: list of items, which need to be operated
        :rtype: list
        """

        _todo_items = set()
        # t??glich zu berechnende Items werden t??glich berechnet
        _todo_items.update(self._daily_items)
        self.tageswert_dict = {}
        self.vortagsendwert_dict = {}
        # wenn Wochentag = Montag, werden auch die w??chentlichen Items berechnet
        if self.shtime.now().hour == 0 and self.shtime.now().minute == 0 and self.shtime.weekday(self.shtime.today()) == 1:
            _todo_items.update(self._weekly_items)
            self.wochenwert_dict = {}
            self.vorwochenendwert_dict = {}
            # wenn erster Tage (des Monates), werden auch die monatlichen Items berechnet
        if self.shtime.now().hour == 0 and self.shtime.now().minute == 0 and self.shtime.now().day == 1:
            _todo_items.update(self._monthly_items)
            self.monatswert_dict = {}
            self.vormonatsendwert_dict = {}
        # wenn erster Tage des ersten Monates, werden auch die j??hrlichen Items berechnet
        if self.shtime.now().hour == 0 and self.shtime.now().minute == 0 and self.shtime.now().day == 1 and self.shtime.now().month == 1:
            _todo_items.update(self._yearly_items)
            self.jahreswert_dict = {}
            self.vorjahresendwert_dict = {}
        return _todo_items

    def _check_db_existence(self) -> bool:
        """
        Check existence of database plugin and database
         - Checks if DB Plugin is loaded and if driver is PyMySql
         - Gets database plugin parameters
         - Puts database connection parameters to plugin properties

        :return: Status of db existance
        :rtype: bool
        """

        # check if database plugin is loaded
        try:
            _db_plugin = self.plugins.return_plugin('database')
        except Exception as e:
            self.logger.error(f"Database plugin not loaded, Error was {e}. No need for DatabaseAddOn Plugin.")
            return False
        else:
            self._db_plugin = _db_plugin

        # get driver of database and check if it is PyMySql to ensure existence of MySql DB
        try:
            self.db_driver = _db_plugin.get_parameter_value('driver')
        except Exception as e:
            self.logger.error(f"Error {e} occurred during getting database plugin parameter 'driver'. DatabaseAddOn Plugin not loaded.")
            return False
        else:
            if self.db_driver.lower() != 'pymysql':
                self.logger.warning(f"Database is of type 'mysql'. Therefore not complete functionality of that plugin given (e.g. db_request not supported).")

        # get database plugin parameters
        try:
            db_instance = _db_plugin.get_parameter_value('instance')
            self.connection_data = _db_plugin.get_parameter_value('connect')  # ['host:localhost', 'user:smarthome', 'passwd:smarthome', 'db:smarthome', 'port:3306']
            self.logger.debug(f"Database Plugin available with instance={db_instance} and connection={self.connection_data}")
        except Exception as e:
            self.logger.error(f"Error {e} occurred during getting database plugin parameters. DatabaseAddOn Plugin not loaded.")
            return False
        else:
            return True

    def _initialize_db(self) -> bool:
        """
        Initializes database connection

        :return: Status of initialization
        :rtype: bool
        """

        try:
            if not self._db.connected():
                # limit connection requests to 20 seconds.
                current_time = time.time()
                time_delta_last_connect = current_time - self.last_connect_time
                # self.logger.debug(f"DEBUG: delta {time_delta_last_connect}")
                if time_delta_last_connect > 20:
                    self.last_connect_time = time.time()
                    self._db.connect()
                else:
                    self.logger.error(f"_initialize_db: Database reconnect suppressed: Delta time: {time_delta_last_connect}")
                    return False
        except Exception as e:
            self.logger.critical(f"_initialize_db: Database: Initialization failed: {e}")
        else:
            return True

    def _get_oldest_log(self, item) -> int:
        """
        Get timestamp of oldest entry of item from cache dict or get value from db and put it to cache dict

        :param item: Item, for which query should be done
        :type item: item

        :return: timestamp of oldest log
        :rtype: int
        """

        if self.prepare_debug:
            self.logger.debug(f"_get_oldest_log: called for item={item.id()}")

        # Zwischenspeicher des oldest_log, zur Reduktion der DB Zugriffe
        if item in self._oldest_log_dict:
            oldest_log = self._oldest_log_dict[item]
        else:
            item_id = self._get_itemid(item)
            oldest_log = self._read_log_oldest(item_id)
            self._oldest_log_dict[item] = oldest_log
        
        if self.prepare_debug:
            self.logger.debug(f"_get_oldest_log for item {item.id()} = {oldest_log}")
        
        return oldest_log

    def _get_oldest_value(self, item) -> Union[int, float, bool]:
        """
        Get value of oldest log of item from cache dict or get value from db and put it to cache dict

        :param item: Item, for which query should be done
        :type item: item

        :return: oldest value
        :rtype: float
        """

        if item in self._oldest_entry_dict and len(self._oldest_entry_dict[item]):
            oldest_value = self._oldest_entry_dict[item][0][4]
        else:
            item_id = self._get_itemid(item)
            validity = False
            i = 0
            while validity is False:
                oldest_entry = self._read_log_timestamp(item_id, self._get_oldest_log(item))
                i += 1
                if isinstance(oldest_entry, list) and isinstance(oldest_entry[0], tuple) and len(oldest_entry[0]) >= 4:
                    self._oldest_entry_dict[item] = oldest_entry
                    oldest_value = oldest_entry[0][4]
                    validity = True
                elif i == 10:
                    oldest_value = -999999999
                    validity = True
                    self.logger.error(f"oldest_value for item {item.id()} could not be read; value is set to -999999999")

        if self.prepare_debug:
            self.logger.debug(f"_get_oldest_value for item {item.id()} = {oldest_value}")
        
        return oldest_value

    def _query_item(self, func: str, item, timespan: str, start: int, end: int, group: str = None, group2: str = None, ignore_value=None) -> Union[int, float, bool, None]:
        """
        Create a mysql query str and param dict based on given parameters, get query response and return it

        :param func: function to be used at query
        :type func: str
        :param item: item object or item_id for which the query should be done
        :type item: item
        :param timespan: time increment f??r definition of start, end, count (day, week, month, year)
        :type timespan: str
        :param start: start of timeframe (oldest) for query given in x time increments (default = None, meaning complete database)
        :type start: int
        :param end: end of timeframe (newest) for query given in x time increments (default = 0, meaning today, end of last week, end of last month, end of last year)
        :type end: int
        :param group: first grouping parameter (default = None, possible values: day, week, month, year)
        :type group: str
        :param group2: second grouping parameter (default = None, possible values: day, week, month, year)
        :type group2: str
        :param ignore_value: value of val_num, which will be ignored during query
        :type ignore_value: num

        :return: query response
        :rtype: float
        """

        convertion = {
            'heute': 'day',
            'woche': 'week',
            'monat': 'month',
            'jahr':  'year',
            'd':     'day',
            'w':     'week',
            'm':     'month',
            'y':     'year'
            }

        if self.prepare_debug:
            self.logger.debug(f"_query_item called with func={func}, item={item.id()}, timespan={timespan}, start={start}, end={end}, group={group}, ignore_value={ignore_value}")

        # get timespan in correct wording
        _timespan = convertion.get(timespan, None)
        _group_new = convertion.get(group, None)
        _group2_new = convertion.get(group2, None)
        if not _timespan:
            return

        # check if start and start is younger then oldest entry
        _dt = datetime.datetime.combine(datetime.date.today(), datetime.datetime.min.time())
        _dt_start = _dt - relativedelta(days=start)
        _dt_end = _dt - relativedelta(days=end)
        if _timespan == 'week':
            _dt_start = _dt - relativedelta(weeks=start)
            _dt_end = _dt - relativedelta(weeks=end)
        elif _timespan == 'month':
            _dt_start = _dt - relativedelta(months=start)
            _dt_end = _dt - relativedelta(months=end)
        elif _timespan == 'year':
            _dt_start = _dt - relativedelta(years=start)
            _dt_end = _dt - relativedelta(years=end)

        _ts_start = int(datetime.datetime.timestamp(_dt_start))
        _ts_end = int(datetime.datetime.timestamp(_dt_end))

        try:
            _oldest_log = int(self._get_oldest_log(item) / 1000)
        except Exception:
            _oldest_log = 0

        if self.prepare_debug:
            self.logger.debug(f"_query_item: oldest_log={_oldest_log}, ts_start={_ts_start}, ts_end={_ts_end}")

        value = None
        # Function 'max1' nur ausf??hren, wenn die Datenbank auch zum Startzeitpunkt der Abfrage auch Werte enth??lt
        if func == 'max1' and _oldest_log > _ts_start:
            self.logger.info(f"_query_item: Requested start time='{_ts_start}' of query with function='{func}' for Item='{item.id()}' is prior to oldest entry='{_oldest_log}'. Query cancelled.")
            return
        # Alle anderen Funktionen nur ausf??hren, wenn die Datenbank auch zum Endzeitpunkt der Abfrage auch Werte enth??lt
        elif _oldest_log > _ts_end:
            self.logger.info(f"_query_item: Requested end time='{_ts_end}' of query with function='{func}' for Item='{item.id()}' is prior to oldest entry='{_oldest_log}'. Query cancelled.")
            return

        log = self._query_log(func, item, _timespan, start=start, end=end, group=_group_new, group2=_group2_new, ignore_value=ignore_value)
        if self.prepare_debug:
            self.logger.debug(f"_query_item: log={log}")

        if log is not None:
            if len(log) == 0:
                value = 0
                if self.prepare_debug:
                    self.logger.debug(f"No entry found database for requested timeframe. Try to get the valid value prior to given point in time.")
            else:
                if log[0][0] is None:
                    self.logger.info(f'No entries for Item {item.id()} in DB found for requested end date. Oldest entry will be used instead')
                    value = int(self._get_oldest_value(item))
                else:
                    if func == 'max1' and len(log) >= 2:
                        try:
                            value = log[1][1] - log[0][1]
                        except Exception:
                            pass
                        if value:
                            value = round(value, 1)
                        else:
                            value = 0
                        if self.prepare_debug:
                            self.logger.debug(f"_query_item: value={value} based on A) with value={log[1][1]} at {timestamp_to_timestring(log[1][0])} and B) with value={log[0][1]} at {timestamp_to_timestring(log[0][0])}")
                    else:
                        value = log[0][1]
                        if value:
                            value = round(value, 1)
                        if self.prepare_debug:
                            self.logger.debug(f"_query_item: value={value} at {timestamp_to_timestring(log[0][0])}")
        else:
            self.logger.error(f"Error occurred during _query_log.")
        
        if self.prepare_debug:
            self.logger.debug(f'_query_item: Results={value} for item={item.id()} with timespan={timespan}, start={start}, end={end}, group_new={_group_new}, group2_new={_group2_new}.')
        
        return value

    def _query_log(self, func: str, item, timespan: str, start: int, end: int, count: int = None, group: str = None, group2: str = None, ignore_value=None):
        """
        Create a mysql query str and param dict based on given parameters, get query response and return it

        :param func: function to be used at query
        :type func: str
        :param item: item object or item_id for which the query should be done
        :type item: item
        :param timespan: time increment f??r definition of start, end, count (day, week, month, year)
        :type timespan: str
        :param start: start of timeframe (oldest) for query given in x time increments (default = None, meaning complete database)
        :type start: int
        :param end: end of timeframe (newest) for query given in x time increments (default = 0, meaning today, end of last week, end of last month, end of last year)
        :type end: int
        :param count: start of timeframe defined by number of time increments starting from end to the left (into the past)
        :type count: int
        :param group: first grouping parameter (default = None, possible values: day, week, month, year)
        :type group: str
        :param group2: second grouping parameter (default = None, possible values: day, week, month, year)
        :type group2: str
        :param ignore_value: value of val_num, which will be ignored during query
        :type ignore_value: num

        :return: query response
        :rtype: tuples
        """
        
        if self.prepare_debug:
            self.logger.debug(f"_query_log: Called with func={func}, item={item.id()}, timespan={timespan}, start={start}, end={end}, count={count}, group={group}, group2={group2}, ignore_value={ignore_value}")

        # create older point in time if count is given but start is not given
        if start is None and count is not None:
            start = int(end) + int(count)
            if not start:
                self.logger.error(f"_query_log: Error occurred during handling of count={count}. Query cancelled.")
                return

        # create item_id from item or string input of item_id,
        if isinstance(item, Item):
            item_id = self._get_itemid(item)
        elif item.isdigit() or isinstance(item, int):
            item_id = int(item)
        else:
            item_id = None
        if not item_id:
            self.logger.error(f"_query_log: ItemId for item={item.id()} not found. Query cancelled.")
            return

        # define query parts
        params = {
            'item':     item_id,
            'end':      int(end),
            'start':    int(start)
            }

        _select = {
            'avg':          'UNIX_TIMESTAMP(DATE(FROM_UNIXTIME(time/1000))) * 1000 as time1, ROUND(AVG(val_num * duration) / AVG(duration), 1) as value ',
            'avg1':         'UNIX_TIMESTAMP(DATE(FROM_UNIXTIME(time/1000))) * 1000 as time1, ROUND(AVG(value), 1) as value ',
            'min':          'UNIX_TIMESTAMP(DATE(FROM_UNIXTIME(time/1000))) * 1000 as time1, ROUND(MIN(val_num), 1) as value ',
            'max':          'UNIX_TIMESTAMP(DATE(FROM_UNIXTIME(time/1000))) * 1000 as time1, ROUND(MAX(val_num), 1) as value ',
            'max1':         'UNIX_TIMESTAMP(DATE(FROM_UNIXTIME(time/1000))) * 1000 as time1, ROUND(MAX(value), 1) as value ',
            'sum':          'UNIX_TIMESTAMP(DATE(FROM_UNIXTIME(time/1000))) * 1000 as time1, ROUND(SUM(val_num), 1) as value ',
            'on':           'UNIX_TIMESTAMP(DATE(FROM_UNIXTIME(time/1000))) * 1000 as time1, ROUND(SUM(val_bool * duration) / SUM(duration), 1) as value ',
            'integrate':    'UNIX_TIMESTAMP(DATE(FROM_UNIXTIME(time/1000))) * 1000 as time1, ROUND(SUM(val_num * duration),1) as value ',
            'sum_max':      'UNIX_TIMESTAMP(DATE(FROM_UNIXTIME(time/1000))) * 1000 as time1, ROUND(SUM(value), 1) as value ',
            'sum_avg':      'UNIX_TIMESTAMP(DATE(FROM_UNIXTIME(time/1000))) * 1000 as time1, ROUND(SUM(value), 1) as value ',
            'sum_min_neg':  'UNIX_TIMESTAMP(DATE(FROM_UNIXTIME(time/1000))) * 1000 as time1, ROUND(SUM(value), 1) as value ',
            'diff_max':     'UNIX_TIMESTAMP(DATE(FROM_UNIXTIME(time/1000))) * 1000 as time1, ROUND(value1 - LAG(value1), 1) OVER (ORDER BY time) AS value '
            }

        _from = {
            'avg':          '',
            'avg1':         'FROM (SELECT time, ROUND(AVG(val_num), 1) as value ',
            'min':          '',
            'max':          '',
            'max1':         'FROM (SELECT time, ROUND(MAX(val_num), 1) as value ',
            'sum':          '',
            'on':           '',
            'integrate':    '',
            'sum_max':      'FROM (SELECT time, ROUND(MAX(val_num), 1) as value ',
            'sum_avg':      'FROM (SELECT time, ROUND(AVG(val_num * duration) / AVG(duration), 1) as value ',
            'sum_min_neg':  'FROM (SELECT time, IF(min(val_num) < 0, ROUND(MIN(val_num), 1), 0) as value ',
            'diff_max':     'FROM (SELECT time, ROUND(MAX(val_num), 1) as value1 '
            }

        # if query is from now (end == 0) until end of database (start is None)
        _where = 'item_id = :item '

        # if query should exclude val_bool
        if func in ['min', 'max', 'max1', 'sum_max', 'sum_avg', 'sum_min_neg', 'diff_max']:
            _where = f'{_where}AND val_bool = 1 '

        if ignore_value:
            _where = f'{_where}AND val_num != {ignore_value} '

        # query from today - x (count) days/weeks/month until y (count) days/weeks/month into the past
        _where_between = {
            'year':  'AND DATE(FROM_UNIXTIME(time/1000)) BETWEEN MAKEDATE(year(now()-interval :start YEAR), 1) AND MAKEDATE(year(now()-interval :end YEAR), 1) ',
            'month': 'AND DATE(FROM_UNIXTIME(time/1000)) BETWEEN DATE_SUB(DATE_ADD(MAKEDATE(YEAR(CURRENT_DATE), 1), INTERVAL MONTH(CURRENT_DATE)-1 MONTH), INTERVAL :start MONTH) AND DATE_SUB(DATE_ADD(MAKEDATE(YEAR(CURRENT_DATE), 1), INTERVAL MONTH(CURRENT_DATE)-1 MONTH), INTERVAL :end MONTH) ',
            'week':  'AND DATE(FROM_UNIXTIME(time/1000)) BETWEEN DATE_SUB(DATE_ADD(CURRENT_DATE, INTERVAL -WEEKDAY(CURRENT_DATE) DAY), INTERVAL :start WEEK) AND DATE_SUB(DATE_ADD(CURRENT_DATE, INTERVAL -WEEKDAY(CURRENT_DATE) DAY), INTERVAL :end WEEK) ',
            'day':   'AND DATE(FROM_UNIXTIME(time/1000)) BETWEEN DATE_SUB(CURDATE(), INTERVAL :start DAY) AND DATE_SUB(CURDATE(), INTERVAL :end DAY) '
            }

        _group_by = {
            'year':  'GROUP BY YEAR(FROM_UNIXTIME(time/1000)) ',
            'month': 'GROUP BY YEAR(FROM_UNIXTIME(time/1000)), MONTH(FROM_UNIXTIME(time/1000)) ',
            'week':  'GROUP BY YEARWEEK(FROM_UNIXTIME(time/1000), 5) ',
            'day':   'GROUP BY DATE(FROM_UNIXTIME(time/1000)) ',
            'hour':  'GROUP BY DATE(FROM_UNIXTIME(time/1000)), HOUR(FROM_UNIXTIME(time/1000)) ',
            None:    ''
            }

        _table_alias = {
            'avg':          '',
            'avg1':         ') AS table1 ',
            'min':          '',
            'max':          '',
            'max1':         ') AS table1 ',
            'sum':          '',
            'on':           '',
            'integrate':    '',
            'sum_max':      ') AS table1 ',
            'sum_avg':      ') AS table1 ',
            'sum_min_neg':  ') AS table1 ',
            'diff_max':     ') AS table1 '
            }

        _from2 = 'log '

        _order = 'time ASC '

        # adapt _where depending in start, end and timespan
        if not (start is None and end == 0):
            _where = f"{_where}{_where_between[timespan]}"

        # check correctness of timespan
        if timespan not in _where_between:
            self.logger.error(f"_query_log: Requested time increment={timespan} for item={item.id()} not defined; Need to be year, month, week, day'. Query cancelled.")
            return

        # check correctness of func
        if func not in _select:
            self.logger.error(f"_query_log: Requested time function={func} for item={item.id()} not defined. Query cancelled.")
            return

        # create query
        query = f"SELECT {_select[func]}{_from[func]}FROM {_from2}WHERE {_where}{_group_by[group]}ORDER BY {_order}{_table_alias[func]}{_group_by[group2]}".strip()

        # do log
        if self.prepare_debug:
            self.logger.debug(f"_query_log: query={query}, params={params}")

        # request database and return result
        return self._fetchall(query, params)

    def _read_log_all(self, item):
        """
        Read the oldest log record for given item

        :param item: Item to read the record for
        :type item: item

        :return: Log record for Item
        """
        
        if self.prepare_debug:
            self.logger.debug(f"_fetch_all_item: Called for item={item}")
        
        if isinstance(item, Item):
            item_id = self._get_itemid(item)
        elif item.isdigit() or isinstance(item, int):
            item_id = int(item)
        else:
            item_id = None

        if item_id:
            query = "SELECT * FROM log WHERE (item_id = :item_id) AND (time = None OR 1 = 1)"
            params = {'item_id': item_id}
            result = self._fetchall(query, params)
            return result

    def _read_log_oldest(self, item_id: int, cur=None):
        """
        Read the oldest log record for given database ID

        :param item_id: Database ID of item to read the record for
        :type item_id: int
        :param cur: A database cursor object if available (optional)

        :return: Log record for the database ID
        """

        params = {'item_id': item_id}
        query = "SELECT min(time) FROM log WHERE item_id = :item_id;"
        return self._fetchall(query, params, cur=cur)[0][0]

    def _read_log_timestamp(self, item_id: int, timestamp: int, cur=None):
        """
        Read database log record for given database ID

        :param item_id: Database ID of item to read the record for
        :type item_id: int
        :param timestamp: timestamp for the given value
        :type timestamp: int
        :param cur: A database cursor object if available (optional)

        :return: Log record for the database ID at given timestamp
        """

        params = {'item_id': item_id, 'timestamp': timestamp}
        query = "SELECT * FROM log WHERE item_id = :item_id AND time = :timestamp;"
        return self._fetchall(query, params, cur=cur)

    def _read_item_table(self, item) -> tuple:
        """
        Read item table if smarthome database

        :param item: name or Item_id of the item within the database
        :type item: item

        :return: Data for the selected item
        :rtype: tuple
        """

        columns_entries = ('id', 'name', 'time', 'val_str', 'val_num', 'val_bool', 'changed')
        columns = ", ".join(columns_entries)

        if isinstance(item, Item):
            query = f"SELECT {columns} FROM item WHERE name = '{str(item.id())}'"
            return self._fetchone(query)

        elif item.isdigit() or isinstance(item, int):
            item = int(item)
            query = f"SELECT {columns} FROM item WHERE id = {item}"
            return self._fetchone(query)

    def _get_db_version(self) -> str:
        """ 
        Query the database version and provide result
        """
        
        query = 'SELECT VERSION()'
        return self._fetchone(query)

    def _get_db_connect_timeout(self) -> str:
        """
        SHOW GLOBAL connect_timeout
        """

        query = "SHOW GLOBAL VARIABLES LIKE 'connect_timeout'"
        return self._fetchone(query)

    def _get_db_net_read_timeout(self) -> str:
        """
        SHOW GLOBAL net_read_timeout
        """

        query = "SHOW GLOBAL VARIABLES LIKE 'net_read_timeout'"
        return self._fetchone(query)

    def _clean_cache_dicts(self) -> None:
        """
        Clean all cache dicts
        """

        self.logger.debug(f"_clean_cache_dicts called. All cache_dicts will be cleaned.")

        # self._itemid_dict = {}
        # self._oldest_log_dict = {}
        # self._oldest_entry_dict = {}
        self.vortagsendwert_dict = {}
        self.vorwochenendwert_dict = {}
        self.vormonatsendwert_dict = {}
        self.vorjahresendwert_dict = {}
        self.tageswert_dict = {}
        self.wochenwert_dict = {}
        self.monatswert_dict = {}
        self.jahreswert_dict = {}

    @property
    def db_version(self):
        return self._get_db_version()

    ##############################
    #   Database specific stuff
    ##############################

    def _execute(self, query: str, params: dict = None, cur=None):
        if params is None:
            params = {}
            
        if self.sql_debug:
            self.logger.debug(f"_execute: Called with query={query}, params={params}")
            
        return self._query(self._db.execute, query, params, cur)

    def _fetchone(self, query: str, params: dict = None, cur=None):
        if params is None:
            params = {}
            
        if self.sql_debug:
            self.logger.debug(f"_fetchone: Called with query={query}, params={params}")
            
        return self._query(self._db.fetchone, query, params, cur)

    def _fetchall(self, query: str, params: dict = None, cur=None):
        if params is None:
            params = {}
            
        if self.sql_debug:
            self.logger.debug(f"_fetchall: Called with query={query}, params={params}")
        
        tuples = self._query(self._db.fetchall, query, params, cur)
        return None if tuples is None else list(tuples)

    def _query(self, fetch, query: str, params: dict = None, cur=None):
        if params is None:
            params = {}
            
        if self.sql_debug:
            self.logger.debug(f"_query: Called with query={query}, params={params}")
            
        if not self._initialize_db():
            return None
            
        if cur is None:
            if self._db.verify(5) == 0:
                self.logger.error("_query: Connection to database not recovered.")
                return None
            if not self._db.lock(300):
                self.logger.error("_query: Can't query due to fail to acquire lock.")
                return None
        
        query_readable = re.sub(r':([a-z_]+)', r'{\1}', query).format(**params)
        
        try:
            tuples = fetch(query, params, cur=cur)
        except Exception as e:
            self.logger.error(f"_query: Error for query {query_readable}: {e}")
        else:
            if self.sql_debug:
                self.logger.debug(f"_query: Result of {query_readable}: {tuples}")
            return tuples
        finally:
            if cur is None:
                self._db.release()


##############################
#      Helper functions
##############################


def params_to_dict(string: str) -> Union[dict, None]:
    """ Parse a string with named arguments and comma separation to dict; (e.g. string = 'year=2022, month=12')
    """

    try:
        res_dict = dict((a.strip(), b.strip()) for a, b in (element.split('=') for element in string.split(', ')))
    except Exception:
        return None
    else:
        # convert to int and remove possible double quotes
        for key in res_dict:
            if isinstance(res_dict[key], str):
                res_dict[key] = res_dict[key].replace('"', '')
                res_dict[key] = res_dict[key].replace("'", "")
            if res_dict[key].isdigit():
                res_dict[key] = int(float(res_dict[key]))

        # check correctness if known key values (func=str, item, timespan=str, start=int, end=int, count=int, group=str, group2=str, year=int, month=int):
        for key in res_dict:
            if key in ('func', 'timespan', 'group', 'group2') and not isinstance(res_dict[key], str):
                return None
            elif key in ('start', 'end', 'count') and not isinstance(res_dict[key], int):
                return None
            elif key in 'year':
                if not valid_year(res_dict[key]):
                    return None
            elif key in 'month':
                if not valid_month(res_dict[key]):
                    return None
        return res_dict


def valid_year(year: Union[int, str]) -> bool:
    """
    Check if given year is digit and within allowed range
    """

    if ((isinstance(year, int) or (isinstance(year, str) and year.isdigit())) and (1980 <= int(year) <= datetime.date.today().year)) or (isinstance(year, str) and year == 'current'):
        return True
    else:
        return False


def valid_month(month: Union[int, str]) -> bool:
    """
    Check if given month is digit and within allowed range
    """

    if (isinstance(month, int) or (isinstance(month, str) and month.isdigit())) and (1 <= int(month) <= 12):
        return True
    else:
        return False


def timestamp_to_timestring(timestamp: int) -> str:
    """
    Parse timestamp from db query to string representing date and time
    """

    return datetime.datetime.fromtimestamp(int(timestamp) / 1000, datetime.timezone.utc).astimezone().strftime('%Y-%m-%d %H:%M:%S')


##############################
#           Backup
##############################
#
# def _delta_value(self, item, time_str_1, time_str_2):
#     """ Computes a difference of values on 2 points in time for an item
#
#     :param item: Item, for which query should be done
#     :param time_str_1: time sting as per database-Plugin for newer point in time (e.g.: 200i)
#     :param time_str_2: Zeitstring gem???? database-Plugin for older point in time(e.g.: 400i)
#     """
#
#     time_since_oldest_log = self._time_since_oldest_log(item)
#     end = int(time_str_1[0:len(time_str_1) - 1])
#
#     if time_since_oldest_log > end:
#         # self.logger.debug(f'_delta_value: fetch DB with {item.id()}.db(max, {time_str_1}, {time_str_1})')
#         value_1 = self._db_plugin._single('max', time_str_1, time_str_1, item.id())
#
#         # self.logger.debug(f'_delta_value: fetch DB with {item.id()}.db(max, {time_str_2}, {time_str_2})')
#         value_2 = self._db_plugin._single('max', time_str_2, time_str_2, item.id())
#
#         if value_1 is not None:
#             if value_2 is None:
#                 self.logger.info(f'No entries for Item {item.id()} in DB found for requested enddate {time_str_1}; try to use oldest entry instead')
#                 value_2 = self._get_oldest_value(item)
#             if value_2 is not None:
#                 value = round(value_1 - value_2, 2)
#                 # self.logger.debug(f'_delta_value for item={item.id()} with time_str_1={time_str_1} and time_str_2={time_str_2} is {value}')
#                 return value
#     else:
#         self.logger.info(f'_delta_value for item={item.id()} using time_str_1={time_str_1} is older as oldest_entry. Therefore no DB request initiated.')
#
# def _single_value(self, item, time_str_1, func='max'):
#     """ Gets value at given point im time from database
#
#     :param item: item, for which query should be done
#     :param time_str_1: time sting as per database-Plugin for point in time (e.g.: 200i)
#     :param func: function of database plugin
#     """
#
#     # value = None
#     # value = item.db(func, time_str_1, time_str_1)
#     value = self._db_plugin._single(func, time_str_1, time_str_1, item.id())
#     if value is None:
#         self.logger.info(f'No entries for Item {item.id()} in DB found for requested end {time_str_1}; try to use oldest entry instead')
#         value = int(self._get_oldest_value(item))
#     # self.logger.debug(f'_single_value for item={item.id()} with time_str_1={time_str_1} is {value}')
#     return value
#
# def _connect_to_db(self, host=None, user=None, password=None, db=None):
#     """ Connect to DB via pymysql
#     """
#
#     if not host:
#         host = self.connection_data[0].split(':', 1)[1]
#     if not user:
#         user = self.connection_data[1].split(':', 1)[1]
#     if not password:
#         password = self.connection_data[2].split(':', 1)[1]
#     if not db:
#         db = self.connection_data[3].split(':', 1)[1]
#     port = self.connection_data[4].split(':', 1)[1]
#
#     try:
#         connection = pymysql.connect(host=host, user=user, password=password, db=db, charset='utf8mb4', cursorclass=pymysql.cursors.DictCursor)
#     except Exception as e:
#         self.logger.error(f"Connection to Database failed with error {e}!.")
#         return
#     else:
#         return connection
#
#
# def _get_itemid_via_db_plugin(self, item):
#     """ Get item_id of item out of dict or request it from db via database plugin and put it into dict
#     """
#
#     # self.logger.debug(f"_get_itemid called for item={item}")
#
#     _item_id = self._itemid_dict.get(item, None)
#     if _item_id is None:
#         _item_id = self._db_plugin.id(item)
#         self._itemid_dict[item] = _item_id
#
#     return _item_id
#
# def _get_time_strs(self, key, x):
#     """ Create timestrings for database query depending in key with
#
#     :param key: key for getting the time strings
#     :param x: time difference as increment
#     :return: tuple of timestrings (timestr closer to now,  timestr more in the past)
#
#     """
#
#     self.logger.debug(f"_get_time_strs called with key={key}, x={x}")
#
#     if key == 'heute':
#         _time_str_1 = self._time_str_heute_minus_x(x - 1)
#         _time_str_2 = self._time_str_heute_minus_x(x)
#     elif key == 'woche':
#         _time_str_1 = self._time_str_woche_minus_x(x - 1)
#         _time_str_2 = self._time_str_woche_minus_x(x)
#     elif key == 'monat':
#         _time_str_1 = self._time_str_monat_minus_x(x - 1)
#         _time_str_2 = self._time_str_monat_minus_x(x)
#     elif key == 'jahr':
#         _time_str_1 = self._time_str_jahr_minus_x(x - 1)
#         _time_str_2 = self._time_str_jahr_minus_x(x)
#     elif key == 'vorjahreszeitraum':
#         _time_str_1 = self._time_str_heute_minus_jahre_x(x + 1)
#         _time_str_2 = self._time_str_jahr_minus_x(x+1)
#     else:
#         _time_str_1 = None
#         _time_str_2 = None
#
#     # self.logger.debug(f"_time_str_1={_time_str_1}, _time_str_2={_time_str_2}")
#     return _time_str_1, _time_str_2
#
# def _time_str_heute_minus_x(self, x=0):
#     """ Creates an str for db request in min from time since beginning of today"""
#     return f"{self.shtime.time_since(self.shtime.today(-x), 'im')}i"
#
# def _time_str_woche_minus_x(self, x=0):
#     """ Creates an str for db request in min from time since beginning of week"""
#     return f"{self.shtime.time_since(self.shtime.beginning_of_week(self.shtime.calendar_week(), None, -x), 'im')}i"
#
# def _time_str_monat_minus_x(self, x=0):
#     """ Creates an str for db request in min for time since beginning of month"""
#     return f"{self.shtime.time_since(self.shtime.beginning_of_month(None, None, -x), 'im')}i"
#
# def _time_str_jahr_minus_x(self, x=0):
#     """ Creates an str for db request in min for time since beginning of year"""
#     return f"{self.shtime.time_since(self.shtime.beginning_of_year(None, -x), 'im')}i"
#
# def _time_str_heute_minus_jahre_x(self, x=0):
#     """ Creates an str for db request in min for time since now x years ago"""
#     return f"{self.shtime.time_since(self.shtime.now() + relativedelta(years=-x), 'im')}i"
#
# def _time_since_oldest_log(self, item):
#     """ Ermittlung der Zeit in ganzen Minuten zwischen "now" und dem ??ltesten Eintrag eines Items in der DB
#
#     :param item: Item, for which query should be done
#     :return: time in minutes from oldest entry to now
#     """
#
#     _timestamp = self._get_oldest_log(item)
#     _oldest_log_dt = datetime.datetime.fromtimestamp(int(_timestamp) / 1000,
#                                                      datetime.timezone.utc).astimezone().strftime(
#         '%Y-%m-%d %H:%M:%S %Z%z')
#     return self.shtime.time_since(_oldest_log_dt, resulttype='im')
#
# @staticmethod
# def _get_dbtimestamp_from_date(date):
#     """ Compute a timestamp for database entry from given date
#
#     :param date: datetime object / string of format 'yyyy-mm'
#     """
#
#     d = None
#     if isinstance(date, datetime.date):
#         d = date
#     elif isinstance(date, str):
#         date = date.split('-')
#         if len(date) == 2:
#             year = int(date[0])
#             month = int(date[1])
#             if (1980 <= year <= datetime.date.today().year) and (1 <= month <= 12):
#                 d = datetime.date(year, month, 1)
#
#     if d:
#         return int(time.mktime(d.timetuple()) * 1000)
#
# def fetch_min_monthly_count(sh, item, count=None):
#     _logger.warning(f"Die Userfunction 'fetch_min_monthly_count' wurde aufgerufen mit item {item} and count {count}")
#
#     if type(item) is str:
#         item = get_item_id(item)
#     if count is None:
#         # query = f"SELECT CONCAT(YEAR(FROM_UNIXTIME(time/1000)), '-', LPAD(MONTH(FROM_UNIXTIME(time/1000)), 2, '0')) AS Date, MIN(val_num) FROM  log WHERE item_id = {item} GROUP BY Date ORDER BY Date ASC"
#         query = f"SELECT time, MIN(val_num) FROM log WHERE item_id = {item} GROUP BY YEAR(FROM_UNIXTIME(time/1000)), MONTH(FROM_UNIXTIME(time/1000)) ORDER BY time ASC"
#     else:
#         # query = f"SELECT CONCAT(YEAR(FROM_UNIXTIME(time/1000)), '-', LPAD(MONTH(FROM_UNIXTIME(time/1000)), 2, '0')) AS Date, MIN(val_num) FROM  log WHERE item_id = {item} AND DATE(FROM_UNIXTIME(time/1000)) > DATE_SUB(now(), INTERVAL {count} MONTH) GROUP BY Date ORDER BY Date ASC"
#         query = f"SELECT time, MIN(val_num) FROM log WHERE item_id = {item} AND DATE(FROM_UNIXTIME(time/1000)) > DATE_SUB(DATE_FORMAT(NOW() ,'%Y-%m-01'), INTERVAL {count} MONTH) GROUP BY YEAR(FROM_UNIXTIME(time/1000)), MONTH(FROM_UNIXTIME(time/1000)) ORDER BY time ASC"
#
#     result = []
#     try:
#         connection = connect_db(sh)
#         with connection.cursor() as cursor:
#             cursor.execute(query)
#             result = cursor.fetchall()
#     finally:
#         connection.close()
#
#     value_list = []
#     for element in result:
#         value_list.append([element['time'], element['MIN(val_num)']])
#
#     _logger.warning(f'mysql.fetch_min_monthly_count value_list: {value_list}')
#     return value_list
#
# def fetch_max_monthly_count(sh, item, count=None):
#     _logger.warning(f"Die Userfunction 'fetch_max_monthly_count' wurde aufgerufen mit item {item} and count {count}")
#
#     if type(item) is str:
#         item = get_item_id(item)
#     if count is None:
#         # query = f"SELECT CONCAT(YEAR(FROM_UNIXTIME(time/1000)), '-', LPAD(MONTH(FROM_UNIXTIME(time/1000)), 2, '0')) AS Date, MAX(val_num) FROM  log WHERE item_id = {item} GROUP BY Date ORDER BY Date ASC"
#         query = f"SELECT time, MAX(val_num) FROM log WHERE item_id = {item} GROUP BY YEAR(FROM_UNIXTIME(time/1000)), MONTH(FROM_UNIXTIME(time/1000)) ORDER BY time ASC"
#     else:
#         # query = f"SELECT CONCAT(YEAR(FROM_UNIXTIME(time/1000)), '-', LPAD(MONTH(FROM_UNIXTIME(time/1000)), 2, '0')) AS Date, MAX(val_num) FROM  log WHERE item_id = {item} AND DATE(FROM_UNIXTIME(time/1000)) > DATE_SUB(now(), INTERVAL {count} MONTH) GROUP BY Date ORDER BY Date ASC"
#         query = f"SELECT time, MAX(val_num), DATE(FROM_UNIXTIME(time/1000)) as DATE FROM log WHERE item_id = {item} AND DATE(FROM_UNIXTIME(time/1000)) > DATE_SUB(DATE_FORMAT(NOW() ,'%Y-%m-01'), INTERVAL {count} MONTH) GROUP BY YEAR(FROM_UNIXTIME(time/1000)), MONTH(FROM_UNIXTIME(time/1000)) ORDER BY time ASC"
#
#     result = []
#     try:
#         connection = connect_db(sh)
#         with connection.cursor() as cursor:
#             cursor.execute(query)
#             result = cursor.fetchall()
#     finally:
#         connection.close()
#
#     _logger.warning(f'mysql.fetch_max_monthly_count result: {result}')
#
#     value_list = []
#     for element in result:
#         value_list.append([element['time'], element['MAX(val_num)']])
#
#     _logger.warning(f'mysql.fetch_max_monthly_count value_list: {value_list}')
#     return value_list
#
# def fetch_avg_monthly_count(sh, item, count=None):
#     _logger.warning(f"Die Userfunction 'fetch_avg_monthly_count' wurde aufgerufen mit item {item} and count {count}")
#
#     if type(item) is str:
#         item = get_item_id(item)
#     if count is None:
#         query = f"SELECT time, ROUND(AVG(val_num * duration) / AVG(duration),2) as AVG FROM log WHERE item_id = {item} GROUP BY YEAR(FROM_UNIXTIME(time/1000)), MONTH(FROM_UNIXTIME(time/1000)) ORDER BY time ASC"
#     else:
#         query = f"SELECT time, ROUND(AVG(val_num * duration) / AVG(duration),2) as AVG FROM log WHERE item_id = {item} AND DATE(FROM_UNIXTIME(time/1000)) > DATE_SUB(DATE_FORMAT(NOW() ,'%Y-%m-01'), INTERVAL {count} MONTH) GROUP BY YEAR(FROM_UNIXTIME(time/1000)), MONTH(FROM_UNIXTIME(time/1000)) ORDER BY time ASC"
#
#     result = []
#     try:
#         connection = connect_db(sh)
#         with connection.cursor() as cursor:
#             cursor.execute(query)
#             result = cursor.fetchall()
#     finally:
#         connection.close()
#
#     value_list = []
#     for element in result:
#         value_list.append([element['time'], element['AVG']])
#
#     _logger.warning(f'mysql.fetch_avg_monthly_count value_list: {value_list}')
#     return value_list
#
# def fetch_min_max_monthly_count(sh, item, count=None):
#     _logger.warning(f"Die Userfunction 'fetch_min_max_monthly_count' wurde aufgerufen mit item {item} and count {count}")
#
#     if type(item) is str:
#         item = get_item_id(item)
#     if count is None:
#         query = f"SELECT CONCAT(YEAR(FROM_UNIXTIME(time/1000)), '-', LPAD(MONTH(FROM_UNIXTIME(time/1000)), 2, '0')) AS Date, MAX(val_num), MIN(val_num) FROM  log WHERE item_id = {item} GROUP BY Date ORDER BY Date DESC"
#     else:
#         query = f"SELECT CONCAT(YEAR(FROM_UNIXTIME(time/1000)), '-', LPAD(MONTH(FROM_UNIXTIME(time/1000)), 2, '0')) AS Date, MAX(val_num), MIN(val_num) FROM  log WHERE item_id = {item} AND DATE(FROM_UNIXTIME(time/1000)) > DATE_SUB(now(), INTERVAL {count} MONTH) GROUP BY Date ORDER BY Date DESC"
#
#     result = []
#     try:
#         connection = connect_db(sh)
#         with connection.cursor() as cursor:
#             cursor.execute(query)
#             result = cursor.fetchall()
#     finally:
#         connection.close()
#     _logger.warning(f'mysql result: {result}')
#     return result
#
# def fetch_min_max_monthly_year(sh, item, year=None):
#     _logger.warning(f"Die Userfunction 'fetch_min_max_monthly_year' wurde aufgerufen mit item {item} and year {year}")
#
#     if type(item) is str:
#         item = get_item_id(item)
#     if year is None:
#         year = datetime.now().year
#
#     query = f"SELECT CONCAT(YEAR(FROM_UNIXTIME(time/1000)), '-', LPAD(MONTH(FROM_UNIXTIME(time/1000)), 2, '0')) AS Date, MAX(val_num), MIN(val_num) FROM log WHERE item_id = {item} AND YEAR(FROM_UNIXTIME(time/1000)) = {year} GROUP BY Date ORDER BY Date DESC"
#     result = []
#     try:
#         connection = connect_db(sh)
#         with connection.cursor() as cursor:
#             cursor.execute(query)
#             result = cursor.fetchall()
#     finally:
#         connection.close()
#     _logger.warning(f'mysql result: {result}')
#     return result
#
# def fetch_min_weekly_count(sh, item, count=None):
#     _logger.warning(f"Die Userfunction 'fetch_min_weekly_count' wurde aufgerufen mit item {item} and count {count}")
#
#     if type(item) is str:
#         item = get_item_id(item)
#     if count is None:
#         count = 51
#     query = f"SELECT time, MIN(val_num), DATE(FROM_UNIXTIME(time/1000)) as DATE FROM log WHERE item_id = {item} AND DATE(FROM_UNIXTIME(time/1000)) > DATE_SUB(DATE_ADD(CURDATE(), INTERVAL - WEEKDAY(CURDATE()) DAY), INTERVAL {count} WEEK) GROUP BY YEAR(FROM_UNIXTIME(time/1000)), WEEK(FROM_UNIXTIME(time/1000)) ORDER BY time ASC"
#     result = []
#     try:
#         connection = connect_db(sh)
#         with connection.cursor() as cursor:
#             cursor.execute(query)
#             result = cursor.fetchall()
#     finally:
#         connection.close()
#
#     value_list = []
#     for element in result:
#         value_list.append([element['time'], element['MIN(val_num)']])
#
#     _logger.warning(f'mysql.fetch_min_weekly_count value_list: {value_list}')
#     return value_list
#
# def fetch_max_weekly_count(sh, item, count=None):
#     _logger.warning(f"Die Userfunction 'fetch_max_weekly_count' wurde aufgerufen mit item {item} and count {count}")
#
#     if type(item) is str:
#         item = get_item_id(item)
#     if count is None:
#         count = 51
#     query = f"SELECT time, MAX(val_num) FROM log WHERE item_id = {item} AND DATE(FROM_UNIXTIME(time/1000)) > DATE_SUB(DATE_ADD(CURDATE(), INTERVAL - WEEKDAY(CURDATE()) DAY), INTERVAL {count} WEEK) GROUP BY YEAR(FROM_UNIXTIME(time/1000)), WEEK(FROM_UNIXTIME(time/1000)) ORDER BY time ASC"
#     result = []
#     try:
#         connection = connect_db(sh)
#         with connection.cursor() as cursor:
#             cursor.execute(query)
#             result = cursor.fetchall()
#     finally:
#         connection.close()
#
#     value_list = []
#     for element in result:
#         value_list.append([element['time'], element['MAX(val_num)']])
#
#     _logger.warning(f'mysql.fetch_max_weekly_count value_list: {value_list}')
#     return value_list
#
# def fetch_avg_weekly_count(sh, item, count=None):
#     _logger.warning(f"Die Userfunction 'fetch_avg_weekly_count' wurde aufgerufen mit item {item} and count {count}")
#
#     if type(item) is str:
#         item = get_item_id(item)
#     if count is None:
#         count = 51
#     query = f"SELECT time, ROUND(AVG(val_num * duration) / AVG(duration),2) as AVG FROM log WHERE item_id = {item} AND DATE(FROM_UNIXTIME(time/1000)) > DATE_SUB(DATE_ADD(CURDATE(), INTERVAL - WEEKDAY(CURDATE()) DAY), INTERVAL {count} WEEK) GROUP BY YEAR(FROM_UNIXTIME(time/1000)), WEEK(FROM_UNIXTIME(time/1000)) ORDER BY time ASC"
#     result = []
#     try:
#         connection = connect_db(sh)
#         with connection.cursor() as cursor:
#             cursor.execute(query)
#             result = cursor.fetchall()
#     finally:
#         connection.close()
#
#     value_list = []
#     for element in result:
#         value_list.append([element['time'], element['AVG']])
#
#     _logger.warning(f'mysql.fetch_avg_weekly_count value_list: {value_list}')
#     return value_list
#
# def fetch_min_max_weekly_count(sh, item, count=None):
#     _logger.warning(f"Die Userfunction 'fetch_min_max_weekly_count' wurde aufgerufen mit item {item} and count {count}")
#
#     if type(item) is str:
#         item = get_item_id(item)
#     if count is None:
#         count = 51
#     query = f"SELECT time, MAX(val_num), MIN(val_num), DATE(FROM_UNIXTIME(time/1000)) as DATE FROM log WHERE item_id = {item} AND DATE(FROM_UNIXTIME(time/1000)) > DATE_SUB(DATE_ADD(CURDATE(), INTERVAL - WEEKDAY(CURDATE()) DAY), INTERVAL {count} WEEK) GROUP BY YEAR(FROM_UNIXTIME(time/1000)), WEEK(FROM_UNIXTIME(time/1000)) ORDER BY time ASC"
#     result = []
#     try:
#         connection = connect_db(sh)
#         with connection.cursor() as cursor:
#             cursor.execute(query)
#             result = cursor.fetchall()
#     finally:
#         connection.close()
#     _logger.warning(f'mysql result: {result}')
#     return result
#
# def fetch_min_max_weekly_year(sh, item, year=None):
#     _logger.warning(f"Die Userfunction 'fetch_min_max_weekly_year' wurde aufgerufen mit item {item} and year {year}")
#
#     if type(item) is str:
#         item = get_item_id(item)
#     if year is None:
#         year = datetime.now().year
#
#     query = f"SELECT CONCAT(YEAR(FROM_UNIXTIME(time/1000)), '/',  LPAD(WEEK(FROM_UNIXTIME(time/1000)), 2, '0')) AS Date, MAX(val_num), MIN(val_num) FROM  log WHERE item_id = {item} AND YEAR(FROM_UNIXTIME(time/1000)) = {year} GROUP BY Date ORDER BY Date DESC"
#     result = []
#     try:
#         connection = connect_db(sh)
#         with connection.cursor() as cursor:
#             cursor.execute(query)
#             result = cursor.fetchall()
#     finally:
#         connection.close()
#     _logger.warning(f'mysql result: {result}')
#     return result
#
# def fetch_min_daily_count(sh, item, count=None):
#     _logger.warning(f"Die Userfunction 'fetch_min_daily_count' wurde aufgerufen mit item {item} as type {type(item)} and count {count}")
#
#     if type(item) is str:
#         item = get_item_id(item)
#     if count is None:
#         count = 30
#
#     query = f"SELECT time, MIN(val_num) FROM log WHERE item_id = {item} AND DATE(FROM_UNIXTIME(time/1000)) > DATE_SUB(now(), INTERVAL {count} DAY) GROUP BY DATE(FROM_UNIXTIME(time/1000)) ORDER BY time ASC"
#     result = []
#     try:
#         connection = connect_db(sh)
#         with connection.cursor() as cursor:
#             cursor.execute(query)
#             result = cursor.fetchall()
#     finally:
#         connection.close()
#
#     value_list = []
#     for element in result:
#         value_list.append([element['time'], element['MIN(val_num)']])
#
#     _logger.warning(f'mysql.fetch_min_daily_count value_list: {value_list}')
#     return value_list
#
# def fetch_max_daily_count(sh, item, count=None):
#     _logger.warning(f"Die Userfunction 'fetch_max_daily_count' wurde aufgerufen mit item {item} as type {type(item)} and count {count}")
#
#     if type(item) is str:
#         item = get_item_id(item)
#     if count is None:
#         count = 30
#
#     query = f"SELECT time, MAX(val_num) FROM log WHERE item_id = {item} AND DATE(FROM_UNIXTIME(time/1000)) > DATE_SUB(now(), INTERVAL {count} DAY) GROUP BY DATE(FROM_UNIXTIME(time/1000)) ORDER BY time ASC"
#     result = []
#     try:
#         connection = connect_db(sh)
#         with connection.cursor() as cursor:
#             cursor.execute(query)
#             result = cursor.fetchall()
#     finally:
#         connection.close()
#
#
#     value_list = []
#     for element in result:
#         value_list.append([element['time'], element['MAX(val_num)']])
#
#     _logger.warning(f'mysql.fetch_max_daily_count value_list: {value_list}')
#     return value_list
#
# def fetch_min_max_daily_count(sh, item, count=None):
#     _logger.warning(f"Die Userfunction 'fetch_min_max_daily_count' wurde aufgerufen mit item {item} as type {type(item)} and count {count}")
#
#     if type(item) is str:
#         item = get_item_id(item)
#     if count is None:
#         count = 30
#
#     query = f"SELECT DATE(FROM_UNIXTIME(time/1000)) AS Date, MAX(val_num), MIN(val_num) FROM  log WHERE item_id = {item} AND DATE(FROM_UNIXTIME(time/1000)) > DATE_SUB(now(), INTERVAL {count} DAY) GROUP BY Date ORDER BY Date DESC"
#     result = []
#     try:
#         connection = connect_db(sh)
#         with connection.cursor() as cursor:
#             cursor.execute(query)
#             result = cursor.fetchall()
#     finally:
#         connection.close()
#     _logger.warning(f'mysql result: {result}')
#     return result
#
# def fetch_min_max_daily_year(sh, item, year=None):
#     _logger.warning(f"Die Userfunction 'fetch_min_max_daily_year' wurde aufgerufen mit item {item} and year {year}")
#
#     if type(item) is str:
#         item = get_item_id(item)
#     if year is None:
#         year = datetime.now().year
#
#     query = f"SELECT DATE(FROM_UNIXTIME(time/1000)) AS Date, MAX(val_num), MIN(val_num) FROM log WHERE item_id = {item} AND YEAR(FROM_UNIXTIME(time/1000)) = {year} GROUP BY Date ORDER BY Date DESC"
#     result = []
#     try:
#         connection = connect_db(sh)
#         with connection.cursor() as cursor:
#             cursor.execute(query)
#             result = cursor.fetchall()
#     finally:
#         connection.close()
#     _logger.warning(f'mysql result: {result}')
#     return result
#
# def _fetch_query(self, query):
#
#     self.logger.debug(f"'_fetch_query'  has been called with query={query}")
#     connection = self._connect_to_db()
#     if connection:
#         try:
#             connection = connect_db(sh)
#             with connection.cursor() as cursor:
#                 cursor.execute(query)
#                 result = cursor.fetchall()
#         except Exception as e:
#             self.logger.error(f"_fetch_query failed with error={e}")
#         else:
#             self.logger.debug(f'_fetch_query result={result}')
#             return result
#         finally:
#             connection.close()
