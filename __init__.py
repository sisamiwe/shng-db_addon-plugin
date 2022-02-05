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
import pymysql.cursors
import datetime
from dateutil.relativedelta import *
import time
import re

########################################
# ToDo
#     'avg'
########################################


class DatabaseAddOn(SmartPlugin):
    """ Main class of the Plugin. Does all plugin specific stuff and provides the update functions for the items
    """

    std_req_dict = {
        'min_monthly_15m': {'func': 'min', 'timespan': 'month', 'count': 15, 'group': 'month'},
        'max_monthly_15m': {'func': 'max', 'timespan': 'month', 'count': 15, 'group': 'month'},
        'avg_monthly_15m': {'func': 'avg', 'timespan': 'month', 'count': 15, 'group': 'month'},
        'min_weekly_30w': {'func': 'min', 'timespan': 'week', 'count': 30, 'group': 'week'},
        'max_weekly_30w': {'func': 'max', 'timespan': 'week', 'count': 30, 'group': 'week'},
        'avg_weekly_30w': {'func': 'avg', 'timespan': 'week', 'count': 30, 'group': 'week'},
        'min_daily_30d': {'func': 'min', 'timespan': 'day', 'count': 30, 'group': 'day'},
        'max_daily_30d': {'func': 'max', 'timespan': 'day', 'count': 30, 'group': 'day'},
        'avg_daily_30d': {'func': 'avg', 'timespan': 'day', 'count': 30, 'group': 'day'},
        'tagesmittelwert': {'func': 'max', 'timespan': 'year', 'start': 0, 'end': 0, 'group': 'day'},
        'tagesmittelwert_hour': {'func': 'avg1', 'timespan': 'day', 'start': 0, 'end': 0, 'group': 'hour', 'group2': 'day'},
        'tagesmittelwert_hour_days': {'func': 'avg1', 'timespan': 'day', 'count': 30, 'group': 'hour', 'group2': 'day'},
        'waermesumme_monthly_24m': {'func': 'sum_max', 'timespan': 'month', 'start': 24, 'end': 0, 'group': 'day', 'group2': 'month'},
        'waermesumme_year_month': {'func': 'sum_max', 'timespan': 'day', 'start': None, 'end': None, 'group': 'day', 'group2': None},
        'kaeltesumme_monthly_24m': {'func': 'sum_max', 'timespan': 'month', 'start': 24, 'end': 0, 'group': 'day', 'group2': 'month'},
        'kaltesumme_year_month': {'func': 'sum_min_neg', 'timespan': 'day', 'start': None, 'end': None, 'group': 'day', 'group2': None},
        'verbrauch_daily_30d': {'func': 'diff_max', 'timespan': 'day', 'count': 30, 'group': 'day'},
        'verbrauch_week_30w': {'func': 'diff_max', 'timespan': 'week', 'count': 30, 'group': 'week'},
        'verbrauch_month_18m': {'func': 'diff_max', 'timespan': 'month', 'count': 18, 'group': 'month'},
        'zaehler_daily_30d': {'func': 'max', 'timespan': 'day', 'count': 30, 'group': 'day'},
        'zaehler_week_30w': {'func': 'max', 'timespan': 'week', 'count': 30, 'group': 'week'},
        'zaehler_month_18m': {'func': 'max', 'timespan': 'month', 'count': 18, 'group': 'month'},
        'gts': {'func': 'max', 'timespan': 'year', 'start': None, 'end': None, 'group': 'day'},
    }

    PLUGIN_VERSION = '1.0.0'

    def __init__(self, sh):
        """ Initializes the plugin.
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
        self._live_items = set()                    # set of items, for which the _database_addon_fct shall be executed on the fly
        self._meter_items = set()                   # set of items, for which the _database_addon_fct shall be executed separately (get create db entry short before midnight)
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
        self._todo_items = set()                    # set of items, witch are due for calculation
        self._db_plugin = None                      # object if database plugin
        self._db = None                             # object of database
        self.connection_data = None                 # connection data list to database
        self.db_driver = None                       # driver for database
        self.last_connect_time = 0                  # mechanism for limiting db connection requests
        self.alive = None
        self.execute_debug = False                  # Enable / Disable debug logging for method 'execute items'
        
        # get plugin parameters
        self.startup_run_delay = self.get_parameter_value('startup_run_delay')
        self.db_instance = self.get_parameter_value('db_instance')

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
            # else:
                # self.logger.debug("Initialization of database API successful")

        if not self._initialize_db():
            self._init_complete = False
            # pass

        # init webinterface
        if not self.init_webinterface(WebInterface):
            self.logger.error(f"Init of WebIF failed. Plugin not loaded")
            self._init_complete = False

    def run(self):
        """ Run method for the plugin
        """

        self.logger.debug("Run method called")
        self.alive = True

        # init db
        # self._initialize_db()
        
        # add scheduler for cyclic trigger item calculation
        self.scheduler_add('cyclic', self.execute_due_items, prio=3, cron='5 0 0 * * *', cycle=None, value=None, offset=None, next=None)
        
        # add scheduler to trigger items to be calculated at startup with delay
        time = self.shtime.now() + datetime.timedelta(seconds=(self.startup_run_delay + 3))
        self.scheduler_add('startup', self.execute_startup_items, next=time)
        # ask for database_version
        self._get_db_version()

    def stop(self):
        """ Stop method for the plugin
        """

        self.logger.debug("Stop method called")
        self.scheduler_remove('cyclic')
        self.alive = False

    def parse_item(self, item):
        """ Default plugin parse_item method. Is called when the plugin is initialized.

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
            self.logger.debug(f"parse item: {item.id()}")

            # get attribute value
            _database_addon_fct = self.get_iattr_value(item.conf, 'database_addon_fct').lower()

            # get attribute if item should be calculated at plugin startup
            if self.has_iattr(item.conf, 'database_addon_startup'):
                _database_addon_startup = self.get_iattr_value(item.conf, 'database_addon_startup')
            else:
                _database_addon_startup = None

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
                self.logger.debug(f"Item '{item.id()}' added with database_addon_fct={_database_addon_fct} and database_item={_database_item.id()}")
                self._item_dict[item] = (_database_addon_fct, _database_item)

                # add item to set of items for time of execution

                # handle items starting with 'zaehlerstand'
                if _database_addon_fct.startswith('zaehlerstand'):
                    self._meter_items.add(item)

                # handle items with 'heute_minus'
                elif 'heute_minus' in _database_addon_fct or _database_addon_fct.startswith('last'):
                    self._daily_items.add(item)

                # handle items with 'woche_minus'
                elif 'woche_minus' in _database_addon_fct:
                    self._weekly_items.add(item)

                # handle items with 'monat_minus'
                elif 'monat_minus' in _database_addon_fct:
                    self._monthly_items.add(item)

                # handle items with 'jahr_minus'
                elif 'jahr_minus' in _database_addon_fct:
                    self._yearly_items.add(item)

                # handle items starting with 'oldest'
                elif _database_addon_fct.startswith('oldest'):
                    self._static_items.add(item)

                # handle items starting with 'vorjahreszeitraum'
                elif _database_addon_fct.startswith('vorjahreszeitraum'):
                    self._daily_items.add(item)

                # handle all functions with 'summe'
                elif 'summe' in _database_addon_fct:
                    if self.db_driver.lower() != 'pymysql':
                        self.logger.warning(f"Functionality of '_database_addon_fct' not given with type of connected database. Item will be ignored.")
                    else:
                        if self.has_iattr(item.conf, 'database_addon_params'):
                            _database_addon_params = parse_params_to_dict(self.get_iattr_value(item.conf, 'database_addon_params'))
                            if _database_addon_params is None:
                                self.logger.warning(f"Error occurred during parsing of item attribute 'database_addon_params' of item {item.id()}. Item will be ignored.")
                            else:
                                if 'year' in _database_addon_params:
                                    _database_addon_params['item'] = _database_item
                                    self._item_dict[item] = self._item_dict[item] + (_database_addon_params,)
                                    self._daily_items.add(item)
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
                            _database_addon_params = parse_params_to_dict(self.get_iattr_value(item.conf, 'database_addon_params'))
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
                                _database_addon_params = parse_params_to_dict(_database_addon_params)
                            if _database_addon_params is None:
                                self.logger.warning(f"Error occurred during parsing of item attribute 'database_addon_params' of item {item.id()}. Item will be ignored.")
                            else:
                                self.logger.debug(f"parse_item: item={item.id()}, _database_addon_params={_database_addon_params}")
                                if any(k in _database_addon_params for k in ('func', 'timespan')):
                                    _database_addon_params['item'] = _database_item
                                    self._item_dict[item] = self._item_dict[item] + (_database_addon_params,)
                                    _timespan = _database_addon_params.get('group', None)
                                    if not _timespan:
                                        _timespan = _database_addon_params.get('timespan', None)
                                        if _timespan == 'day':
                                            self._daily_items.add(item)
                                        elif _timespan == 'week':
                                            self._weekly_items.add(item)
                                        elif _timespan == 'month':
                                            self._monthly_items.add(item)
                                        elif _timespan == 'year':
                                            self._yearly_items.add(item)
                                        else:
                                            self.logger.warning(f"Item '{item.id()}' with database_addon_fct={_database_addon_fct} ignored. Not able to detect update cycle.")
                                else:
                                    self.logger.warning(f"Item '{item.id()}' with database_addon_fct={_database_addon_fct} ignored, not all mandatory parameters in database_addon_params={_database_addon_params} given. Item will be ignored.")
                        else:
                            self.logger.warning(f"Item '{item.id()}' with database_addon_fct={_database_addon_fct} ignored, since parameter using 'database_addon_params' not given. Item will be ignored")

                # handle live items
                else:
                    self._live_items.add(item)
                    self._database_items.add(_database_item)

            if _database_addon_startup is not None and _database_item is not None:
                self.logger.debug(f"Item '{item.id()}' added to be run on startup")
                self._startup_items.add(item)

        # Callback mit 'update_item' für alle Items mit Attribut 'database', um die live Items zu berechnen
        elif self.has_iattr(item.conf, 'database'):
            return self.update_item

    def update_item(self, item, caller=None, source=None, dest=None):
        """ Handle updates item

        This method is called, if the value of an item has been updated by SmartHomeNG.
        It should write the changed value out to the device (hardware/interface) that
        is managed by this plugin.

        :param item: item to be updated towards the plugin
        :param caller: if given it represents the callers name
        :param source: if given it represents the source
        :param dest: if given it represents the dest
        """

        if self.alive and caller != self.get_shortname():
            # self.logger.info(f"Update item: {item.property.path}, item has been changed outside this plugin")
            if item in self._database_items:
                self.logger.debug(f"update_item was called with item {item.property.path} with value {item()} from caller {caller}, source {source} and dest {dest}")
                self._fill_cache_dicts(item, item())

    def execute_due_items(self):
        """ Handle scheduler call, ask for due items and start execute_items
        """

        self.logger.debug("execute_due_items called")

        _todo_items = self._create_due_items()
        self.logger.info(f"execute_due_items: Following items will be calculated: {_todo_items}")

        self.execute_items(_todo_items)
        
    def execute_startup_items(self):
        """ Handle scheduler call to execute startup items
        """

        self.logger.debug("execute_startup_items called")
        self.execute_items(self._startup_items)
        
    def execute_all_items(self):
        """ Handle scheduler call to execute all items
        """

        self.logger.debug("execute_all_items called")
        self.execute_items(list(self._item_dict.keys()))

    def execute_items(self, item_list):
        """ Execute functions per item based on given item list
        """

        self.logger.debug(f"execute_items called with item_list={item_list}")

        for item in item_list:
            _database_addon_fct = self._item_dict[item][0]
            _database_addon_params = None
            _database_item = self._item_dict[item][1]
            _var = _database_addon_fct.split('_')
            _time_str_1 = None
            _time_str_2 = None
            _result = None

            self.logger.debug(f"execute_items: item '{item}' is due with _database_addon_fct={_database_addon_fct} _database_item={_database_item.id()}")

            # handle oldest_value
            if _database_addon_fct == 'oldest_value':
                _result = self._get_oldest_value(_database_item)

            # handle oldest_log
            elif _database_addon_fct == 'oldest_log':
                _result = self._get_oldest_log(_database_item)

            # handle zaehlerstand_heute
            elif _database_addon_fct == 'zaehlerstand_heute':
                _result = _database_item.property.value

            # handle db_version
            elif _database_addon_fct == 'db_version':
                _result = self._get_db_version()

            # handle kaeltesumme, waermesumme, gruendlandtempsumme
            elif 'summe' in _database_addon_fct:
                _database_addon_params = self._item_dict[item][2]

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
                _database_addon_params = self._item_dict[item][2]

                if self.execute_debug:
                    self.logger.debug(f"execute_items: _database_addon_fct={_database_addon_fct} detected; _database_addon_params={_database_addon_params}")

                if _database_addon_params.keys() & {'item'}:
                    _result = self.tagesmitteltemperatur(**_database_addon_params)

            # handle db_request
            elif _database_addon_fct == 'db_request':
                _database_addon_params = self._item_dict[item][2]

                if self.execute_debug:
                    self.logger.debug(f"execute_items: _database_addon_fct={_database_addon_fct} detected; _database_addon_params={_database_addon_params}")

                if _database_addon_params.keys() & {'func', 'item', 'timespan'}:
                    _result = self.fetch_log(**_database_addon_params)
                else:
                    self.logger.error(f"Attribute 'database_addon_params' not containing needed params for Item {item.id} with _database_addon_fct={_database_addon_fct}.")

            # handle all live functions of format 'timeframe_function' like 'heute_max'
            elif len(_var) == 2 and _var[1] in ['min', 'max']:
                self.logger.info(f"execute_items: on_change function={_var[0]} with {_var[1]} detected; will be calculated by next change of database item")

            # handle all live functions of format 'timeframe' like 'heute'
            elif len(_var) == 1 and _var[0]:
                self.logger.info(f"execute_items: on_change function={_var} detected; will be calculated by next change of database item")

            # handle all functions starting with last in format 'last_window_function' like 'last_24h_max'
            elif len(_var) == 3 and _var[0] == 'last':
                _window = _var[1]
                _func = _var[2]

                if self.execute_debug:
                    self.logger.debug(f"execute_items: 'last' function detected. _window={_window}, _func={_func}")

                if _window[-1:] in ['i', 'h', 'd', 'w', 'm', 'y']:
                    if _window[:-1].isdigit():
                        _result = _database_item.db(_func, _window)

            # handle all functions 'wertehistorie min/max/avg' in format 'timeframe_timedelta_func' like 'heute_minus2_max'
            elif len(_var) == 3 and _var[2] in ['min', 'max', 'avg']:
                _timeframe = _var[0]
                _timedelta = _var[1][-1]
                _func = _var[2]

                if self.execute_debug:
                    self.logger.debug(f"execute_items: _database_addon_fct={_func} detected; _timeframe={_timeframe}, _timedelta={_timedelta}")

                if _timedelta.isdigit():
                    _timedelta = int(_timedelta)
                    _time_str_1, _time_str_2 = self._get_time_strs(_timeframe, _timedelta)
                    if _time_str_1 is not None and _time_str_2 is not None:
                        _result = self._value_of_db_function(_database_item, _func, _time_str_1, _time_str_2)

            # handle all functions 'wertehistorie total' in format 'timeframe_timedelta' like 'heute_minus2'
            elif len(_var) == 2 and _var[1].startswith('minus'):
                _timeframe = _var[0]
                _timedelta = _var[1][-1]

                if self.execute_debug:
                    self.logger.debug(f"execute_items: '{_database_addon_fct}' function detected. _timeframe={_timeframe}, _timedelta={_timedelta}")

                if _timedelta.isdigit():
                    _timedelta = int(_timedelta)
                    _time_str_1, _time_str_2 = self._get_time_strs(_timeframe, _timedelta)
                    if _time_str_1 is not None and _time_str_1 is not None:
                        _result = self._delta_value(_database_item, _time_str_1, _time_str_2)

            # handle all functions of format 'function_timeframe_timedelta' like 'zaehlerstand_woche_minus1'
            elif len(_var) == 3 and _var[2].startswith('minus'):
                _func = _var[0]
                _timeframe = _var[1]
                _timedelta = _var[2][-1]

                if self.execute_debug:
                    self.logger.debug(f"execute_items: {_func} function detected. _timeframe={_timeframe}, _timedelta={_timedelta}")

                if _timedelta.isdigit():
                    _timedelta = int(_timedelta)
                    if _func == 'zaehlerstand':
                        _time_str_1, _time_str_2 = self._get_time_strs(_timeframe, _timedelta)
                        if _time_str_1 is not None:
                            _result = self._single_value(_database_item, _time_str_1)

            # handle all functions of format 'function_window_timeframe_timedelta' like 'rolling_12m_woche_minus1'
            elif len(_var) == 4 and _var[3].startswith('minus'):
                _func = _var[0]
                _window = _var[1]
                _timeframe = _var[2]
                _timedelta = _var[3][-1]

                if self.execute_debug:
                    self.logger.debug(f"execute_items: {_func} function detected. _window={_window}  _timeframe={_timeframe}, _timedelta={_timedelta}")

                if _timedelta.isdigit():
                    _timedelta = int(_timedelta)
                    if _func == 'rolling':
                        if _timeframe == 'woche':
                            _time_str_1 = self._time_str_heute_minus_x(0)
                            _time_str_2 = self._time_str_heute_minus_x(365)
                        elif _timeframe == 'monat':
                            _time_str_1 = self._time_str_monat_minus_x(0)
                            _time_str_2 = self._time_str_monat_minus_x(12)
                        elif _timeframe == 'jahr':
                            _time_str_1 = self._time_str_jahr_minus_x(0)
                            _time_str_2 = self._time_str_jahr_minus_x(1)
                        if _time_str_1 is not None and _time_str_2 is not None:
                            _result = self._delta_value(_database_item, _time_str_1, _time_str_2)

            # handle everything else
            else:
                self.logger.warning(f"execute_items: Function {_database_addon_fct } for item {item.id()} not defined or found")

            if self.execute_debug:
                self.logger.debug(f"execute_items: result is {_result} for item '{item.id()}' with _database_addon_fct={_database_addon_fct} _database_item={_database_item} _database_addon_params={_database_addon_params}")

            # set item value
            if _result is not None:
                # if fetch_log provides Error Message as result as list starting with '<ERROR>', put this into error log
                if isinstance(_result, list) and '<ERROR>' in _result:
                    self.logger.error(f"execute_items: result with error received. {_result}")
                else:
                    item(_result, self.get_shortname())

    ##############################
    #       Public functions
    ##############################

    def gruenlandtemperatursumme(self, item, year):
        """ Calculates the Grünlandtemperatursumme for given item and year
        """
        if not valid_year(year):
            return
        
        year = int(year)
        current_year = datetime.date.today().year
        year_delta = current_year - year

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

    def waermesumme(self, item, year, month=None):
        """ Calculates the Wärmesumme for given item, year and month
        """
        if not valid_year(year):
            return

        if month is None:
            start_date = datetime.date(int(year), 3, 20)
            end_date = datetime.date(int(year), 9, 21)
            group2 = 'year'
        elif valid_month(month):
            start_date = datetime.date(int(year), int(month), 1)
            end_date = start_date + relativedelta(months=+1) - datetime.timedelta(days=1)
            group2 = 'month'
        else:
            return
        today = datetime.date.today()
        start = (today - start_date).days
        end = (today - end_date).days if end_date < today else 0

        _database_addon_params = self.std_req_dict.get('waermesumme_year_month', None)
        _database_addon_params['start'] = start
        _database_addon_params['end'] = end
        if group2:
            _database_addon_params['group2'] = group2
        _database_addon_params['item'] = item

        result = self.fetch_log(**_database_addon_params)

        if result:
            if month is None:
                result = result[0][1]
            return result

    def kaeltesumme(self, item, year, month=None):
        """ Calculates the Kältesumme for given item, year and month
        """
        if not valid_year(year):
            return
        
        if month is None:
            start_date = datetime.date(int(year), 9, 21)
            end_date = datetime.date(int(year) + 1, 3, 22)
            group2 = None
        elif valid_month(month):
            start_date = datetime.date(int(year), int(month), 1)
            end_date = start_date + relativedelta(months=+1) - datetime.timedelta(days=1)
            group2 = 'month'
        else:
            return

        today = datetime.date.today()
        start = (today - start_date).days
        end = (today - end_date).days if end_date < today else 0

        _database_addon_params = self.std_req_dict.get('kaltesumme_year_month', None)
        _database_addon_params['start'] = start
        _database_addon_params['end'] = end
        if group2:
            _database_addon_params['group2'] = group2
        _database_addon_params['item'] = item

        result = self.fetch_log(**_database_addon_params)

        if result:
            if month is None:
                result = result[0][1]
            return result

    def tagesmitteltemperatur(self, item, count=None):
        
        _database_addon_params = self.std_req_dict.get('tagesmittelwert_hour_days', None)
        if count:
            _database_addon_params['count'] = count
        _database_addon_params['item'] = item

        result = self.fetch_log(**_database_addon_params)
        
        return result

    def fetch_log(self, func, item, timespan, start=None, end=0, count=None, group=None, group2=None):
        """ Create a mysql query str and param dict based on given paramaters, get query response, and transfer response to list

        :param func: function to be used at query
        :type func: str
        :param item: item object or item_id for which the query should be done
        :type item: item
        :param timespan: time increment für definition of start, end, count (day, week, month, year)
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

        :return: query response
        :rtype: list

        """

        self.logger.debug(f"fetch_log: Called with func={func}, item={item.id()}, timespan={timespan}, start={start}, end={end}, count={count}, group={group}")

        # create older point in time if count is given but start is not given
        if start is None and count is not None:
            start = int(end) + int(count)
            if not start:
                return ['<ERROR>: Error occurred during handling of count.']

        # create item_id from item or string input of item_id,
        if isinstance(item, Item):
            item_id = self._get_itemid(item)
        elif item.isdigit() or isinstance(item, int):
            item_id = int(item)
        else:
            item_id = None
        if not item_id:
            return ['<ERROR>: Was not able to detect item_id.']

        # define query parts
        param_dict = {
            'item': item_id,
            'end': int(end),
            'start': int(start)
            }

        select = {
            'avg': 'UNIX_TIMESTAMP(DATE(FROM_UNIXTIME(time/1000))) * 1000 as time1, ROUND(AVG(val_num * duration) / AVG(duration), 1) as value',
            'avg1': 'UNIX_TIMESTAMP(DATE(FROM_UNIXTIME(time/1000))) * 1000 as time1, ROUND(AVG(value), 2) as value FROM (SELECT time, ROUND(AVG(val_num), 2) as value',
            'min': 'UNIX_TIMESTAMP(DATE(FROM_UNIXTIME(time/1000))) * 1000 as time1, MIN(val_num) as value',
            'max': 'UNIX_TIMESTAMP(DATE(FROM_UNIXTIME(time/1000))) * 1000 as time1, MAX(val_num) as value',
            'sum': 'UNIX_TIMESTAMP(DATE(FROM_UNIXTIME(time/1000))) * 1000 as time1, SUM(val_num) as value',
            'on': 'UNIX_TIMESTAMP(DATE(FROM_UNIXTIME(time/1000))) * 1000 as time1, ROUND(SUM(val_bool * duration) / SUM(duration), 1) as value',
            'integrate': 'UNIX_TIMESTAMP(DATE(FROM_UNIXTIME(time/1000))) * 1000 as time1, ROUND(SUM(val_num * duration),1) as value',
            'sum_max': 'UNIX_TIMESTAMP(DATE(FROM_UNIXTIME(time/1000))) * 1000 as time1, ROUND(SUM(value), 1) as value FROM (SELECT time, MAX(val_num) as value',
            'sum_avg': 'UNIX_TIMESTAMP(DATE(FROM_UNIXTIME(time/1000))) * 1000 as time1, ROUND(SUM(value), 1) as value FROM (SELECT time, ROUND(AVG(val_num * duration) / AVG(duration), 1) as value',
            'sum_min_neg': 'UNIX_TIMESTAMP(DATE(FROM_UNIXTIME(time/1000))) * 1000 as time1, ROUND(SUM(value), 1) as value FROM (SELECT time, IF(min(val_num) < 0, min(val_num), 0) as value',
            'diff_max': 'UNIX_TIMESTAMP(DATE(FROM_UNIXTIME(time/1000))) * 1000 as time1, value1 - LAG(value1) OVER (ORDER BY time) AS value FROM ( SELECT time, round(MAX(val_num), 2) as value1'
            }

        # if query is from now (end == 0) until end of database (start is None)
        if start is None and end == 0:
            where = {
                    'year': 'item_id = :item',
                    'month': 'item_id = :item',
                    'week': 'item_id = :item',
                    'day': 'item_id = :item'
                    }

        # query from today - x (count) days/weeks/month until y (count) days/weeks/month into the past
        else:
            where = {
                    'year': 'item_id = :item AND DATE(FROM_UNIXTIME(time/1000)) BETWEEN MAKEDATE(year(now()-interval :start year),1) AND DATE_SUB(CURRENT_DATE, INTERVAL :end YEAR)',
                    'month': 'item_id = :item AND DATE(FROM_UNIXTIME(time/1000)) BETWEEN DATE_SUB(DATE_ADD(MAKEDATE(YEAR(CURRENT_DATE), 1), INTERVAL MONTH(CURRENT_DATE)-1 MONTH), INTERVAL :start MONTH) AND DATE_SUB(CURRENT_DATE, INTERVAL :end MONTH)',
                    'week': 'item_id = :item AND DATE(FROM_UNIXTIME(time/1000)) BETWEEN DATE_SUB(DATE_ADD(CURRENT_DATE, INTERVAL -WEEKDAY(CURRENT_DATE) DAY), INTERVAL :start WEEK) AND DATE_SUB(CURRENT_DATE, INTERVAL :end WEEK)',
                    'day': 'item_id = :item AND DATE(FROM_UNIXTIME(time/1000)) BETWEEN DATE_SUB(CURDATE(), INTERVAL :start DAY) AND DATE_SUB(CURDATE(), INTERVAL :end DAY)'
                    }

        group_by = {
                    'year': 'GROUP BY YEAR(FROM_UNIXTIME(time/1000))',
                    'month': 'GROUP BY YEAR(FROM_UNIXTIME(time/1000)), MONTH(FROM_UNIXTIME(time/1000))',
                    'week': 'GROUP BY YEARWEEK(FROM_UNIXTIME(time/1000), 5)',
                    'day': 'GROUP BY DATE(FROM_UNIXTIME(time/1000))',
                    'hour': 'GROUP BY DATE(FROM_UNIXTIME(time/1000)), HOUR(FROM_UNIXTIME(time/1000))',
                    None: ''
                    }

        table_alias = {
                    'avg': '',
                    'avg1': ') AS table1',
                    'min': '',
                    'max': '',
                    'sum': '',
                    'on': '',
                    'integrate': '',
                    'sum_max': ') AS table1',
                    'sum_avg': ') AS table1',
                    'sum_min_neg': ') AS table1',
                    'diff_max': ') AS table1'
                    }

        # check correctness of timespan
        if timespan not in where:
            return ['<ERROR>: Requested time increment not defined; Need to be year, month, week, day']

        # check correctness of func
        if func not in select:
            return ['<ERROR>: Requested function is not defined.']

        # create query
        query = f"SELECT {select[func]} FROM log WHERE {where[timespan]} {group_by[group]} ORDER BY time ASC {table_alias[func]} {group_by[group2]}"

        # do log
        self.logger.debug(f"fetch_log: query={query}, param_dict={param_dict}")

        # request database
        result = self._fetchall(query, param_dict)

        # handle result
        value = []
        for element in result:
            value.append([element[0], element[1]])
        if func == 'diff_max':
            value.pop(0)
        self.logger.debug(f"fetch_log: value for item={item} with timespan={timespan}, func={func}: {value}")
        return value

    def fetch_log_raw(self, query, param_dict=None):

        formatted_sql = sqlvalidator.format_sql(query)
        sql_query = sqlvalidator.parse(formatted_sql)

        if not sql_query.is_valid():
            self.logger.error(f"fetch_log_raw: Validation of query failed with error: {sql_query.errors}")
            return

        # request database
        result = self._fetchall(query, param_dict)

        return result

    ##############################
    #        Support stuff
    ##############################

    def _fill_cache_dicts(self, updated_item, value):
        """ Get item and item value for which an update has been detected, fill cache dicts and set item value.
        """

        map_dict = {
            'heute': (self.tageswert_dict, self._time_str_heute_minus_x()),
            'woche': (self.wochenwert_dict, self._time_str_woche_minus_x()),
            'monat': (self.monatswert_dict, self._time_str_monat_minus_x()),
            'jahr': (self.jahreswert_dict, self._time_str_jahr_minus_x())
            }

        map_dict1 = {
            'heute': (self.vortagsendwert_dict, self._time_str_heute_minus_x()),
            'woche': (self.vorwochenendwert_dict, self._time_str_woche_minus_x()),
            'monat': (self.vormonatsendwert_dict, self._time_str_monat_minus_x()),
            'jahr': (self.vorjahresendwert_dict, self._time_str_jahr_minus_x())
            }

        for item in self._live_items:
            _database_item = self._item_dict[item][1]
            if _database_item == updated_item:
                _database_addon_fct = self._item_dict[item][0]
                _var = _database_addon_fct.split('_')

                # handle heute_max, heute_min, woche_max, woche_min.....
                if len(_var) == 2 and _var[1] in ['min', 'max']:
                    _timeframe = _var[0]
                    _func = _var[1]
                    _cache_dict, _time_str = map_dict[_timeframe]

                    # update cache dicts
                    if _database_item not in _cache_dict:
                        _cache_dict[_database_item] = {}
                    if _cache_dict[_database_item].get(_func, None) is None:
                        value = _database_item.db(_func, _time_str)
                        self.logger.debug(f"_fill_cache_dicts: Item={updated_item.id()} with _func={_func} and _timeframe={_timeframe} not in cache dict. Value {value} will be added.")
                        _cache_dict[_database_item][_func] = value

                    _update = False
                    if _func == 'min' and value < _cache_dict[_database_item][_func]:
                        _update = True
                    elif _func == 'max' and value > _cache_dict[_database_item][_func]:
                        _update = True
                    if _update:
                        _cache_dict[_database_item][_func] = value

                    # check value is float ending of .0 and convert to int
                    if isinstance(value, str) and value.is_integer():
                        value = int(value)

                    # set item value
                    if value != item():
                        item(value, self.get_shortname())

                # handle heute, woche, monat, jahr
                elif len(_var) == 1:
                    _timeframe = _var[0]
                    _cache_dict, _time_str = map_dict1[_timeframe]

                    # update cache dicts
                    if _database_item not in _cache_dict:
                        value = self._single_value(_database_item, _time_str)
                        self.logger.debug(f"_fill_cache_dicts: Item={updated_item.id()} with _timeframe={_timeframe} not in cache dict. Value {value} will be added.")
                        _cache_dict[_database_item] = value

                    # calculate value
                    delta_value = round(value - _cache_dict[_database_item], 2)

                    # check value is float ending of .0 and convert to int
                    if isinstance(delta_value, str) and delta_value.is_integer():
                        delta_value = int(delta_value)

                    # set item value
                    if delta_value != item():
                        item(delta_value, self.get_shortname())

    def _get_itemid(self, item):
        """ Get item_id of item out of dict or request it from db via database plugin and put it into dict
        """

        # self.logger.debug(f"_get_itemid called for item={item}")

        _item_id = self._itemid_dict.get(item, None)
        if _item_id is None:
            _item_id = self._db_plugin.id(item)
            self._itemid_dict[item] = _item_id

        return _item_id

    def _create_due_items(self):
        """ Create list of items which are due, resets cache dicts
        """

        _todo_items = set()
        # täglich zu berechnende Items werden täglich berechnet
        _todo_items.update(self._daily_items)
        self.tageswert_dict = {}
        self.vortagsendwert_dict = {}
        # wenn Wochentag = Montag, werden auch die wöchentlichen Items berechnet
        if self.shtime.now().hour == 0 and self.shtime.now().minute == 0 and self.shtime.weekday(self.shtime.today()) == 1:
            _todo_items.update(self._weekly_items)
            self.wochenwert_dict = {}
            self.vorwochenendwert_dict = {}
            # wenn erster Tage (des Monates), werden auch die monatlichen Items berechnet
        if self.shtime.now().hour == 0 and self.shtime.now().minute == 0 and self.shtime.now().day == 1:
            _todo_items.update(self._montly_items)
            self.monatswert_dict = {}
            self.vormonatsendwert_dict = {}
        # wenn erster Tage des ersten Monates, werden auch die jährlichen Items berechnet
        if self.shtime.now().hour == 0 and self.shtime.now().minute == 0 and self.shtime.now().day == 1 and self.shtime.now().month == 1:
            _todo_items.update(self._yearly_items)
            self.jahreswert_dict = {}
            self.vorjahresendwert_dict = {}
        return _todo_items

    def _check_db_existence(self):
        """ Check existence of database plugin and database

         - Checks if DB Plugin is loaded and if driver is PyMySql
         - Gets database plugin parameters
         - Puts database connection parameters to plugin properties
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

    def _initialize_db(self):
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

    def _get_oldest_log(self, item):
        """ Get timestamp of oldest entry of item from cache dict or get value from db and put it to cache dict

        :param item: Item, for which query should be done
        :return: timestamp of oldest log
        """

        # self.logger.debug(f"_get_oldest_log: called for item={item.id()}")

        # Zwischenspeicher des oldest_log, zur Reduktion der DB Zugriffe
        if item in self._oldest_log_dict:
            oldest_log = self._oldest_log_dict[item]
        else:
            item_id = self._get_itemid(item)
            oldest_log = self._db_plugin.readOldestLog(item_id)
            self._oldest_log_dict[item] = oldest_log

        # self.logger.debug(f"_get_oldest_log for item {item.id()} = {oldest_log}")
        return oldest_log

    def _get_oldest_value(self, item):
        """ Get value of oldest log of item from cache dict or get value from db and put it to cache dict

        :param item: Item, for which query should be done
        :return: oldest value
        """

        if item in self._oldest_entry_dict:
            oldest_entry = self._oldest_entry_dict[item]
        else:
            item_id = self._get_itemid(item)
            oldest_entry = self._db_plugin.readLog(item_id, self._get_oldest_log(item))
            self._oldest_entry_dict[item] = oldest_entry

        # self.logger.debug(f"_get_oldest_value for item {item.id()} = {self._oldest_entry_dict[item][0][4]}")
        return oldest_entry[0][4]

    def _time_since_oldest_log(self, item):
        """ Ermittlung der Zeit in ganzen Minuten zwischen "now" und dem ältesten Eintrag eines Items in der DB

        :param item: Item, for which query should be done
        :return: time in minutes from oldest entry to now
        """

        _timestamp = self._get_oldest_log(item)
        _oldest_log_dt = datetime.datetime.fromtimestamp(int(_timestamp) / 1000, datetime.timezone.utc).astimezone().strftime('%Y-%m-%d %H:%M:%S %Z%z')
        return self.shtime.time_since(_oldest_log_dt, resulttype='im')

    def _delta_value(self, item, time_str_1, time_str_2):
        """ Computes a difference of values on 2 points in time for an item

        :param item: Item, for which query should be done
        :param time_str_1: time sting as per database-Plugin for newer point in time (e.g.: 200i)
        :param time_str_2: Zeitstring gemäß database-Plugin for older point in time(e.g.: 400i)
        """

        time_since_oldest_log = self._time_since_oldest_log(item)
        end = int(time_str_1[0:len(time_str_1) - 1])

        if time_since_oldest_log > end:
            # self.logger.debug(f'_delta_value: fetch DB with {item.id()}.db(max, {time_str_1}, {time_str_1})')
            value_1 = item.db('max', time_str_1, time_str_1)
            # self.logger.debug(f'_delta_value: fetch DB with {item.id()}.db(max, {time_str_2}, {time_str_2})')
            value_2 = item.db('max', time_str_2, time_str_2)
            if value_1 is not None:
                if value_2 is None:
                    self.logger.info(f'No entries for Item {item.id()} in DB found for requested enddate {time_str_1}; try to use oldest entry instead')
                    value_2 = self._get_oldest_value(item)
                if value_2 is not None:
                    value = round(value_1 - value_2, 2)
                    # self.logger.debug(f'_delta_value for item={item.id()} with time_str_1={time_str_1} and time_str_2={time_str_2} is {value}')
                    return value
        else:
            self.logger.info(f'_delta_value for item={item.id()} using time_str_1={time_str_1} is older as oldest_entry. Therefore no DB request initiated.')

    def _single_value(self, item, time_str_1, func='max'):
        """ Gets value at given point im time from database

        :param item: item, for which query should be done
        :param time_str_1: time sting as per database-Plugin for point in time (e.g.: 200i)
        :param func: function of database plugin
        """

        # value = None
        value = item.db(func, time_str_1, time_str_1)
        if value is None:
            self.logger.info(f'No entries for Item {item.id()} in DB found for requested end {time_str_1}; try to use oldest entry instead')
            value = int(self._get_oldest_value(item))
        # self.logger.debug(f'_single_value for item={item.id()} with time_str_1={time_str_1} is {value}')
        return value

    def _value_of_db_function(self, item, func, time_str_1, time_str_2):
        """ Query the database using database plugin functionality

        :param item: item, for which query should be done
        :param func: function of database plugin
        :param time_str_1: time sting as per database-Plugin for newer point in time (e.g.: 200i)
        :param time_str_2: Zeitstring gemäß database-Plugin for older point in time(e.g.: 400i)
        """

        # self.logger.debug(f"_value_of_db_function called with item={item.id()}, func={func}, time_str_1={time_str_1} and time_str_2={time_str_2}")

        value = item.db(func, time_str_1, time_str_2)
        if value is None:
            self.logger.info(f'_value_of_db_function: No entries for Item {item} in DB found for requested startdate {time_str_1}; try to use oldest entry instead')
            time_since_oldest_log = self._time_since_oldest_log(item)
            end = int(time_str_2[0:len(time_str_2) - 1])
            if time_since_oldest_log > end:
                time_str_1 = f"{self._time_since_oldest_log(item)}i"
                value = item.db(func, time_str_1, time_str_2)
            else:
                self.logger.info(f"_value_of_db_function for item={item.id()}: 'time_since_oldest_log' <= 'end'")
        # self.logger.debug(f'_value_of_db_function for item={item.id()} with function={func}, time_str_1={time_str_1}, time_str_2={time_str_2} is {value}')
        return value

    def _get_time_strs(self, key, x):
        """ Create timestrings for database query depending in key with

        :param key: key for getting the time strings
        :param x: time difference as increment
        :return: tuple of timestrings (timestr closer to now,  timestr more in the past)

        """

        self.logger.debug(f"_get_time_strs called with key={key}, x={x}")

        if key == 'heute':
            _time_str_1 = self._time_str_heute_minus_x(x - 1)
            _time_str_2 = self._time_str_heute_minus_x(x)
        elif key == 'woche':
            _time_str_1 = self._time_str_woche_minus_x(x - 1)
            _time_str_2 = self._time_str_woche_minus_x(x)
        elif key == 'monat':
            _time_str_1 = self._time_str_monat_minus_x(x - 1)
            _time_str_2 = self._time_str_monat_minus_x(x)
        elif key == 'jahr':
            _time_str_1 = self._time_str_jahr_minus_x(x - 1)
            _time_str_2 = self._time_str_jahr_minus_x(x)
        elif key == 'vorjahreszeitraum':
            _time_str_1 = self._time_str_heute_minus_jahre_x(x + 1)
            _time_str_2 = self._time_str_jahr_minus_x(x+1)
        else:
            _time_str_1 = None
            _time_str_2 = None

        # self.logger.debug(f"_time_str_1={_time_str_1}, _time_str_2={_time_str_2}")
        return _time_str_1, _time_str_2

    def _time_str_heute_minus_x(self, x=0):
        """ Creates an str for db request in min from time since beginning of today"""
        return f"{self.shtime.time_since(self.shtime.today(-x), 'im')}i"

    def _time_str_woche_minus_x(self, x=0):
        """ Creates an str for db request in min from time since beginning of week"""
        return f"{self.shtime.time_since(self.shtime.beginning_of_week(self.shtime.calendar_week(), None, -x), 'im')}i"

    def _time_str_monat_minus_x(self, x=0):
        """ Creates an str for db request in min for time since beginning of month"""
        return f"{self.shtime.time_since(self.shtime.beginning_of_month(None, None, -x), 'im')}i"

    def _time_str_jahr_minus_x(self, x=0):
        """ Creates an str for db request in min for time since beginning of year"""
        return f"{self.shtime.time_since(self.shtime.beginning_of_year(None, -x), 'im')}i"

    def _time_str_heute_minus_jahre_x(self, x=0):
        """ Creates an str for db request in min for time since now x years ago"""
        return f"{self.shtime.time_since(self.shtime.now() + relativedelta(years=-x), 'im')}i"

    def _fetch_all_item(self, item):
        """ Query the database version and provide result
        The fetchAll method retrieves all (remaining) rows of a query result, returning them as a sequence of sequences.
        
        """

        self.logger.debug(f"_fetch_all_item: Called for item={item}")
        if isinstance(item, Item):
            item_id = self._get_itemid(item)
        elif item.isdigit() or isinstance(item, int):
            item_id = int(item)
        else:
            item_id = None
            
        if item_id:
            query = "select * from log where (item_id=%s) AND (time = None OR 1 = 1)"
            param_dict = {'item_id': item_id}
            result = self._fetchall(query, param_dict)
            return result

    def _get_db_version(self):
        """ Query the database version and provide result
        """
        query = 'SELECT VERSION()'

        return self._fetchone(query)

    @property
    def db_version(self):
        return self._get_db_version()

    ##############################
    #   Database specific stuff
    ##############################

    def _execute(self, query, params, cur=None):
        self._query(self._db.execute, query, params, cur)

    def _fetchone(self, query, params={}, cur=None):
        # self.logger.debug(f"_fetchone: Called with query={query}, params={params}")
        return self._query(self._db.fetchone, query, params, cur)

    def _fetchall(self, query, params={}, cur=None):
        # self.logger.debug(f"_fetchall: Called with query={query}, params={params}")
        tuples = self._query(self._db.fetchall, query, params, cur)
        return None if tuples is None else list(tuples)

    def _query(self, func, query, params, cur=None):
        if not self._initialize_db():
            return None
        if cur is None:
            if self._db.verify(5) == 0:
                self.logger.error("_query: Database: Connection not recovered")
                return None
            if not self._db.lock(300):
                self.logger.error("_query: Database: Can't query due to fail to acquire lock")
                return None
        query_readable = re.sub(r':([a-z_]+)', r'{\1}', query).format(**params)
        try:
            tuples = func(query, params, cur=cur)
        except Exception as e:
            self.logger.error(f"_query: Database: Error for query {query_readable}: {e}")
            raise e
        else:
            self.logger.debug(f"_query: Database: Fetch {query_readable}: {tuples}")
            return tuples
        finally:
            if cur is None:
                self._db.release()

    ##############################
    #           Backup
    ##############################

    def _connect_to_db(self, host=None, user=None, password=None, db=None):
        """ Connect to DB via pymysql
        """

        if not host:
            host = self.connection_data[0].split(':', 1)[1]
        if not user:
            user = self.connection_data[1].split(':', 1)[1]
        if not password:
            password = self.connection_data[2].split(':', 1)[1]
        if not db:
            db = self.connection_data[3].split(':', 1)[1]
        port = self.connection_data[4].split(':', 1)[1]

        try:
            connection = pymysql.connect(host=host, user=user, password=password, db=db, charset='utf8mb4', cursorclass=pymysql.cursors.DictCursor)
        except Exception as e:
            self.logger.error(f"Connection to Database failed with error {e}!.")
            return
        else:
            return connection

    def _read_item_table(self, item):
        """ Read item table if smarthome database

        :param item: name or Item_id of the item within the database
        :return: Data for the selected item
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

    def _get_itemid_direct(self, item):
        """ Returns the ID of the given item from cache dict or request it directly from database

        :param item: Item to get the ID for
        :return: id of the item within the database
        :rtype: int | None
        """
        self.logger.debug(f"_get_itemid_direct: Called with item={item.id()}")

        _item_id = self._itemid_dict.get(item, None)
        if _item_id is None:
            row = self._read_item_table(item)
            if row:
                if len(row) > 0:
                    _item_id = int(row[0])
                    self._itemid_dict[item] = _item_id
        return _item_id

    @staticmethod
    def _get_dbtimestamp_from_date(date):
        """ Compute a timestamp for database entry from given date

        :param date: datetime object / string of format 'yyyy-mm'
        """

        d = None
        if isinstance(date, datetime.date):
            d = date
        elif isinstance(date, str):
            date = date.split('-')
            if len(date) == 2:
                year = int(date[0])
                month = int(date[1])
                if (1980 <= year <= datetime.date.today().year) and (1 <= month <= 12):
                    d = datetime.date(year, month, 1)

        if d:
            return int(time.mktime(d.timetuple()) * 1000)

##############################
#      Helper functions
##############################


def parse_params_to_dict(string):
    """ Parse a string with named arguments and comma separation to dict; string = 'year=2022, month=12'
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


def valid_year(year):
    if (isinstance(year, int) or (isinstance(year, str) and year.isdigit())) and (1980 <= year <= datetime.date.today().year):
        return True
    else:
        return False


def valid_month(month):
    if (isinstance(month, int) or (isinstance(month, str) and month.isdigit())) and (1 <= month <= 12):
        return True
    else:
        return False

##############################
#           Backup
##############################


"""
def fetch_min_monthly_count(sh, item, count=None):
    _logger.warning(f"Die Userfunction 'fetch_min_monthly_count' wurde aufgerufen mit item {item} and count {count}")

    if type(item) is str:
        item = get_item_id(item)
    if count is None:
        # query = f"SELECT CONCAT(YEAR(FROM_UNIXTIME(time/1000)), '-', LPAD(MONTH(FROM_UNIXTIME(time/1000)), 2, '0')) AS Date, MIN(val_num) FROM  log WHERE item_id = {item} GROUP BY Date ORDER BY Date ASC"
        query = f"SELECT time, MIN(val_num) FROM log WHERE item_id = {item} GROUP BY YEAR(FROM_UNIXTIME(time/1000)), MONTH(FROM_UNIXTIME(time/1000)) ORDER BY time ASC"
    else:
        # query = f"SELECT CONCAT(YEAR(FROM_UNIXTIME(time/1000)), '-', LPAD(MONTH(FROM_UNIXTIME(time/1000)), 2, '0')) AS Date, MIN(val_num) FROM  log WHERE item_id = {item} AND DATE(FROM_UNIXTIME(time/1000)) > DATE_SUB(now(), INTERVAL {count} MONTH) GROUP BY Date ORDER BY Date ASC"
        query = f"SELECT time, MIN(val_num) FROM log WHERE item_id = {item} AND DATE(FROM_UNIXTIME(time/1000)) > DATE_SUB(DATE_FORMAT(NOW() ,'%Y-%m-01'), INTERVAL {count} MONTH) GROUP BY YEAR(FROM_UNIXTIME(time/1000)), MONTH(FROM_UNIXTIME(time/1000)) ORDER BY time ASC"

    result = []
    try:
        connection = connect_db(sh)
        with connection.cursor() as cursor:
            cursor.execute(query)
            result = cursor.fetchall()
    finally:
        connection.close()

    value_list = []
    for element in result:
        value_list.append([element['time'], element['MIN(val_num)']])

    _logger.warning(f'mysql.fetch_min_monthly_count value_list: {value_list}')
    return value_list

def fetch_max_monthly_count(sh, item, count=None):
    _logger.warning(f"Die Userfunction 'fetch_max_monthly_count' wurde aufgerufen mit item {item} and count {count}")

    if type(item) is str:
        item = get_item_id(item)
    if count is None:
        # query = f"SELECT CONCAT(YEAR(FROM_UNIXTIME(time/1000)), '-', LPAD(MONTH(FROM_UNIXTIME(time/1000)), 2, '0')) AS Date, MAX(val_num) FROM  log WHERE item_id = {item} GROUP BY Date ORDER BY Date ASC"
        query = f"SELECT time, MAX(val_num) FROM log WHERE item_id = {item} GROUP BY YEAR(FROM_UNIXTIME(time/1000)), MONTH(FROM_UNIXTIME(time/1000)) ORDER BY time ASC"
    else:
        # query = f"SELECT CONCAT(YEAR(FROM_UNIXTIME(time/1000)), '-', LPAD(MONTH(FROM_UNIXTIME(time/1000)), 2, '0')) AS Date, MAX(val_num) FROM  log WHERE item_id = {item} AND DATE(FROM_UNIXTIME(time/1000)) > DATE_SUB(now(), INTERVAL {count} MONTH) GROUP BY Date ORDER BY Date ASC"
        query = f"SELECT time, MAX(val_num), DATE(FROM_UNIXTIME(time/1000)) as DATE FROM log WHERE item_id = {item} AND DATE(FROM_UNIXTIME(time/1000)) > DATE_SUB(DATE_FORMAT(NOW() ,'%Y-%m-01'), INTERVAL {count} MONTH) GROUP BY YEAR(FROM_UNIXTIME(time/1000)), MONTH(FROM_UNIXTIME(time/1000)) ORDER BY time ASC"

    result = []
    try:
        connection = connect_db(sh)
        with connection.cursor() as cursor:
            cursor.execute(query)
            result = cursor.fetchall()
    finally:
        connection.close()

    _logger.warning(f'mysql.fetch_max_monthly_count result: {result}')

    value_list = []
    for element in result:
        value_list.append([element['time'], element['MAX(val_num)']])

    _logger.warning(f'mysql.fetch_max_monthly_count value_list: {value_list}')
    return value_list

def fetch_avg_monthly_count(sh, item, count=None):
    _logger.warning(f"Die Userfunction 'fetch_avg_monthly_count' wurde aufgerufen mit item {item} and count {count}")

    if type(item) is str:
        item = get_item_id(item)
    if count is None:
        query = f"SELECT time, ROUND(AVG(val_num * duration) / AVG(duration),2) as AVG FROM log WHERE item_id = {item} GROUP BY YEAR(FROM_UNIXTIME(time/1000)), MONTH(FROM_UNIXTIME(time/1000)) ORDER BY time ASC"
    else:
        query = f"SELECT time, ROUND(AVG(val_num * duration) / AVG(duration),2) as AVG FROM log WHERE item_id = {item} AND DATE(FROM_UNIXTIME(time/1000)) > DATE_SUB(DATE_FORMAT(NOW() ,'%Y-%m-01'), INTERVAL {count} MONTH) GROUP BY YEAR(FROM_UNIXTIME(time/1000)), MONTH(FROM_UNIXTIME(time/1000)) ORDER BY time ASC"

    result = []
    try:
        connection = connect_db(sh)
        with connection.cursor() as cursor:
            cursor.execute(query)
            result = cursor.fetchall()
    finally:
        connection.close()

    value_list = []
    for element in result:
        value_list.append([element['time'], element['AVG']])

    _logger.warning(f'mysql.fetch_avg_monthly_count value_list: {value_list}')
    return value_list

def fetch_min_max_monthly_count(sh, item, count=None):
    _logger.warning(f"Die Userfunction 'fetch_min_max_monthly_count' wurde aufgerufen mit item {item} and count {count}")

    if type(item) is str:
        item = get_item_id(item)
    if count is None:
        query = f"SELECT CONCAT(YEAR(FROM_UNIXTIME(time/1000)), '-', LPAD(MONTH(FROM_UNIXTIME(time/1000)), 2, '0')) AS Date, MAX(val_num), MIN(val_num) FROM  log WHERE item_id = {item} GROUP BY Date ORDER BY Date DESC"
    else:
        query = f"SELECT CONCAT(YEAR(FROM_UNIXTIME(time/1000)), '-', LPAD(MONTH(FROM_UNIXTIME(time/1000)), 2, '0')) AS Date, MAX(val_num), MIN(val_num) FROM  log WHERE item_id = {item} AND DATE(FROM_UNIXTIME(time/1000)) > DATE_SUB(now(), INTERVAL {count} MONTH) GROUP BY Date ORDER BY Date DESC"

    result = []
    try:
        connection = connect_db(sh)
        with connection.cursor() as cursor:
            cursor.execute(query)
            result = cursor.fetchall()
    finally:
        connection.close()
    _logger.warning(f'mysql result: {result}')
    return result

def fetch_min_max_monthly_year(sh, item, year=None):
    _logger.warning(f"Die Userfunction 'fetch_min_max_monthly_year' wurde aufgerufen mit item {item} and year {year}")

    if type(item) is str:
        item = get_item_id(item)
    if year is None:
        year = datetime.now().year

    query = f"SELECT CONCAT(YEAR(FROM_UNIXTIME(time/1000)), '-', LPAD(MONTH(FROM_UNIXTIME(time/1000)), 2, '0')) AS Date, MAX(val_num), MIN(val_num) FROM log WHERE item_id = {item} AND YEAR(FROM_UNIXTIME(time/1000)) = {year} GROUP BY Date ORDER BY Date DESC"
    result = []
    try:
        connection = connect_db(sh)
        with connection.cursor() as cursor:
            cursor.execute(query)
            result = cursor.fetchall()
    finally:
        connection.close()
    _logger.warning(f'mysql result: {result}')
    return result

def fetch_min_weekly_count(sh, item, count=None):
    _logger.warning(f"Die Userfunction 'fetch_min_weekly_count' wurde aufgerufen mit item {item} and count {count}")

    if type(item) is str:
        item = get_item_id(item)
    if count is None:
        count = 51
    query = f"SELECT time, MIN(val_num), DATE(FROM_UNIXTIME(time/1000)) as DATE FROM log WHERE item_id = {item} AND DATE(FROM_UNIXTIME(time/1000)) > DATE_SUB(DATE_ADD(CURDATE(), INTERVAL - WEEKDAY(CURDATE()) DAY), INTERVAL {count} WEEK) GROUP BY YEAR(FROM_UNIXTIME(time/1000)), WEEK(FROM_UNIXTIME(time/1000)) ORDER BY time ASC"
    result = []
    try:
        connection = connect_db(sh)
        with connection.cursor() as cursor:
            cursor.execute(query)
            result = cursor.fetchall()
    finally:
        connection.close()

    value_list = []
    for element in result:
        value_list.append([element['time'], element['MIN(val_num)']])

    _logger.warning(f'mysql.fetch_min_weekly_count value_list: {value_list}')
    return value_list

def fetch_max_weekly_count(sh, item, count=None):
    _logger.warning(f"Die Userfunction 'fetch_max_weekly_count' wurde aufgerufen mit item {item} and count {count}")

    if type(item) is str:
        item = get_item_id(item)
    if count is None:
        count = 51
    query = f"SELECT time, MAX(val_num) FROM log WHERE item_id = {item} AND DATE(FROM_UNIXTIME(time/1000)) > DATE_SUB(DATE_ADD(CURDATE(), INTERVAL - WEEKDAY(CURDATE()) DAY), INTERVAL {count} WEEK) GROUP BY YEAR(FROM_UNIXTIME(time/1000)), WEEK(FROM_UNIXTIME(time/1000)) ORDER BY time ASC"
    result = []
    try:
        connection = connect_db(sh)
        with connection.cursor() as cursor:
            cursor.execute(query)
            result = cursor.fetchall()
    finally:
        connection.close()

    value_list = []
    for element in result:
        value_list.append([element['time'], element['MAX(val_num)']])

    _logger.warning(f'mysql.fetch_max_weekly_count value_list: {value_list}')
    return value_list

def fetch_avg_weekly_count(sh, item, count=None):
    _logger.warning(f"Die Userfunction 'fetch_avg_weekly_count' wurde aufgerufen mit item {item} and count {count}")

    if type(item) is str:
        item = get_item_id(item)
    if count is None:
        count = 51
    query = f"SELECT time, ROUND(AVG(val_num * duration) / AVG(duration),2) as AVG FROM log WHERE item_id = {item} AND DATE(FROM_UNIXTIME(time/1000)) > DATE_SUB(DATE_ADD(CURDATE(), INTERVAL - WEEKDAY(CURDATE()) DAY), INTERVAL {count} WEEK) GROUP BY YEAR(FROM_UNIXTIME(time/1000)), WEEK(FROM_UNIXTIME(time/1000)) ORDER BY time ASC"
    result = []
    try:
        connection = connect_db(sh)
        with connection.cursor() as cursor:
            cursor.execute(query)
            result = cursor.fetchall()
    finally:
        connection.close()

    value_list = []
    for element in result:
        value_list.append([element['time'], element['AVG']])

    _logger.warning(f'mysql.fetch_avg_weekly_count value_list: {value_list}')
    return value_list

def fetch_min_max_weekly_count(sh, item, count=None):
    _logger.warning(f"Die Userfunction 'fetch_min_max_weekly_count' wurde aufgerufen mit item {item} and count {count}")

    if type(item) is str:
        item = get_item_id(item)
    if count is None:
        count = 51
    query = f"SELECT time, MAX(val_num), MIN(val_num), DATE(FROM_UNIXTIME(time/1000)) as DATE FROM log WHERE item_id = {item} AND DATE(FROM_UNIXTIME(time/1000)) > DATE_SUB(DATE_ADD(CURDATE(), INTERVAL - WEEKDAY(CURDATE()) DAY), INTERVAL {count} WEEK) GROUP BY YEAR(FROM_UNIXTIME(time/1000)), WEEK(FROM_UNIXTIME(time/1000)) ORDER BY time ASC"
    result = []
    try:
        connection = connect_db(sh)
        with connection.cursor() as cursor:
            cursor.execute(query)
            result = cursor.fetchall()
    finally:
        connection.close()
    _logger.warning(f'mysql result: {result}')
    return result

def fetch_min_max_weekly_year(sh, item, year=None):
    _logger.warning(f"Die Userfunction 'fetch_min_max_weekly_year' wurde aufgerufen mit item {item} and year {year}")

    if type(item) is str:
        item = get_item_id(item)
    if year is None:
        year = datetime.now().year

    query = f"SELECT CONCAT(YEAR(FROM_UNIXTIME(time/1000)), '/',  LPAD(WEEK(FROM_UNIXTIME(time/1000)), 2, '0')) AS Date, MAX(val_num), MIN(val_num) FROM  log WHERE item_id = {item} AND YEAR(FROM_UNIXTIME(time/1000)) = {year} GROUP BY Date ORDER BY Date DESC"
    result = []
    try:
        connection = connect_db(sh)
        with connection.cursor() as cursor:
            cursor.execute(query)
            result = cursor.fetchall()
    finally:
        connection.close()
    _logger.warning(f'mysql result: {result}')
    return result

def fetch_min_daily_count(sh, item, count=None):
    _logger.warning(f"Die Userfunction 'fetch_min_daily_count' wurde aufgerufen mit item {item} as type {type(item)} and count {count}")

    if type(item) is str:
        item = get_item_id(item)
    if count is None:
        count = 30

    query = f"SELECT time, MIN(val_num) FROM log WHERE item_id = {item} AND DATE(FROM_UNIXTIME(time/1000)) > DATE_SUB(now(), INTERVAL {count} DAY) GROUP BY DATE(FROM_UNIXTIME(time/1000)) ORDER BY time ASC"
    result = []
    try:
        connection = connect_db(sh)
        with connection.cursor() as cursor:
            cursor.execute(query)
            result = cursor.fetchall()
    finally:
        connection.close()

    value_list = []
    for element in result:
        value_list.append([element['time'], element['MIN(val_num)']])

    _logger.warning(f'mysql.fetch_min_daily_count value_list: {value_list}')
    return value_list

def fetch_max_daily_count(sh, item, count=None):
    _logger.warning(f"Die Userfunction 'fetch_max_daily_count' wurde aufgerufen mit item {item} as type {type(item)} and count {count}")

    if type(item) is str:
        item = get_item_id(item)
    if count is None:
        count = 30

    query = f"SELECT time, MAX(val_num) FROM log WHERE item_id = {item} AND DATE(FROM_UNIXTIME(time/1000)) > DATE_SUB(now(), INTERVAL {count} DAY) GROUP BY DATE(FROM_UNIXTIME(time/1000)) ORDER BY time ASC"
    result = []
    try:
        connection = connect_db(sh)
        with connection.cursor() as cursor:
            cursor.execute(query)
            result = cursor.fetchall()
    finally:
        connection.close()


    value_list = []
    for element in result:
        value_list.append([element['time'], element['MAX(val_num)']])

    _logger.warning(f'mysql.fetch_max_daily_count value_list: {value_list}')
    return value_list

def fetch_min_max_daily_count(sh, item, count=None):
    _logger.warning(f"Die Userfunction 'fetch_min_max_daily_count' wurde aufgerufen mit item {item} as type {type(item)} and count {count}")

    if type(item) is str:
        item = get_item_id(item)
    if count is None:
        count = 30

    query = f"SELECT DATE(FROM_UNIXTIME(time/1000)) AS Date, MAX(val_num), MIN(val_num) FROM  log WHERE item_id = {item} AND DATE(FROM_UNIXTIME(time/1000)) > DATE_SUB(now(), INTERVAL {count} DAY) GROUP BY Date ORDER BY Date DESC"
    result = []
    try:
        connection = connect_db(sh)
        with connection.cursor() as cursor:
            cursor.execute(query)
            result = cursor.fetchall()
    finally:
        connection.close()
    _logger.warning(f'mysql result: {result}')
    return result

def fetch_min_max_daily_year(sh, item, year=None):
    _logger.warning(f"Die Userfunction 'fetch_min_max_daily_year' wurde aufgerufen mit item {item} and year {year}")

    if type(item) is str:
        item = get_item_id(item)
    if year is None:
        year = datetime.now().year

    query = f"SELECT DATE(FROM_UNIXTIME(time/1000)) AS Date, MAX(val_num), MIN(val_num) FROM log WHERE item_id = {item} AND YEAR(FROM_UNIXTIME(time/1000)) = {year} GROUP BY Date ORDER BY Date DESC"
    result = []
    try:
        connection = connect_db(sh)
        with connection.cursor() as cursor:
            cursor.execute(query)
            result = cursor.fetchall()
    finally:
        connection.close()
    _logger.warning(f'mysql result: {result}')
    return result

def _fetch_query(self, query):

    self.logger.debug(f"'_fetch_query'  has been called with query={query}")
    connection = self._connect_to_db()
    if connection:
        try:
            connection = connect_db(sh)
            with connection.cursor() as cursor:
                cursor.execute(query)
                result = cursor.fetchall()
        except Exception as e:
            self.logger.error(f"_fetch_query failed with error={e}")
        else:
            self.logger.debug(f'_fetch_query result={result}')
            return result
        finally:
            connection.close()

"""
