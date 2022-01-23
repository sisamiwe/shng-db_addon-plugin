#!/usr/bin/env python3
# vim: set encoding=utf-8 tabstop=4 softtabstop=4 shiftwidth=4 expandtab
#########################################################################
#  Copyright 2020-      <AUTHOR>                                  <EMAIL>
#########################################################################
#  This file is part of SmartHomeNG.
#  https://www.smarthomeNG.de
#  https://knx-user-forum.de/forum/supportforen/smarthome-py
#
#  Sample plugin for new plugins to run with SmartHomeNG version 1.8 and
#  upwards.
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

import pymysql.cursors
import datetime
from dateutil.relativedelta import *
import time
# import json


class DatabaseAddOn(SmartPlugin):
    """
    Main class of the Plugin. Does all plugin specific stuff and provides
    the update functions for the items

    HINT: Please have a look at the SmartPlugin class to see which
    class properties and methods (class variables and class functions)
    are already available!
    """

    PLUGIN_VERSION = '1.0.0'    # (must match the version specified in plugin.yaml), use '1.0.0' for your initial plugin Release

    def __init__(self, sh):
        """
        Initalizes the plugin.

        If you need the sh object at all, use the method self.get_sh() to get it. There should be almost no need for
        a reference to the sh object any more.

        Plugins have to use the new way of getting parameter values:
        use the SmartPlugin method get_parameter_value(parameter_name). Anywhere within the Plugin you can get
        the configured (and checked) value for a parameter by calling self.get_parameter_value(parameter_name). It
        returns the value in the datatype that is defined in the metadata.
        """

        # Call init code of parent class (SmartPlugin)
        super().__init__()

        # get item and shtime
        self.shtime = Shtime.get_instance()
        self.items = Items.get_instance()
        self.plugins = Plugins.get_instance()

        # define properties
        self._item_dict = {}                        # dict to hold all items {item1: ('_database_addon_fct', '_database_item'), item2: ('_database_addon_fct', '_database_item')...}
        self._daily_items = set()                   # set of items, for which the _database_addon_fct shall be executed daily
        self._weekly_items = set()                  # set of items, for which the _database_addon_fct shall be executed weekly
        self._monthly_items = set()                 # set of items, for which the _database_addon_fct shall be executed monthly
        self._yearly_items = set()                  # set of items, for which the _database_addon_fct shall be executed yearly
        self._live_items = set()                    # set of items, for which the _database_addon_fct shall be executed on the fly
        self._meter_items = set()                   # set of items, for which the _database_addon_fct shall be executed separatly (get create db entry short before midnight)
        self._startup_items = set()                 # set of items, for which the _database_addon_fct shall be executed on startup
        self._database_items = set()                # set of items with database attribut, relevant for plugin
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
        self._db_plugin = None
        self._db_host = None
        self._db_user = None
        self._db_pw = None
        self._db_db = None
        self.alive = None

        # check existance of db-plugin, get parameters
        if not self._check_db_existance():
            self.logger.error(f"Check of existence of database plugin incl connection check failed. Plugin not loaded")
            self._init_complete = False

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
        self.scheduler_add('daily', self.execute_due_items, prio=3, cron='1 0 0 * * *', cycle=None, value=None, offset=None, next=None)
        self.execute_items(self._startup_items)
        self._get_db_version()

    def stop(self):
        """
        Stop method for the plugin
        """
        self.logger.debug("Stop method called")
        self.scheduler_remove('poll_device')
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
            self.logger.debug(f"parse item: {item.id()}")
            
            # get attribut value
            _database_addon_fct = self.get_iattr_value(item.conf, 'database_addon_fct').lower()

            # get attribut if item should be calculated at plugin startup
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
                if _database_addon_fct.startswith('zaehlerstand'):
                    self._meter_items.add(item)
                elif 'heute_minus' in _database_addon_fct or 'summe' in _database_addon_fct:
                    self._daily_items.add(item)
                elif 'woche_minus' in _database_addon_fct:
                    self._weekly_items.add(item)
                elif 'monat_minus' in _database_addon_fct:
                    self._monthly_items.add(item)
                elif 'jahr_minus' in _database_addon_fct:
                    self._yearly_items.add(item)
                elif _database_addon_fct.startswith('oldest'):
                    self._static_items.add(item)
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
        """
        Item has been updated

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

    def _fill_cache_dicts(self, updated_item, value):

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
                    if not _cache_dict[_database_item].get(_func, None):
                        _cache_dict[_database_item][_func] = _database_item.db(_func, _time_str)
                    else:
                        _update = False
                        if _func == 'min' and value < _cache_dict[_database_item][_func]:
                            _update = True
                        elif _func == 'max' and value > _cache_dict[_database_item][_func]:
                            _update = True
                        if _update:
                            _cache_dict[_database_item][_func] = value

                    # set item value
                    if value != item():
                        item(value, self.get_shortname())

                # handle heute, woche, monat, jahr
                elif len(_var) == 1:
                    _timeframe = _var[0]
                    _cache_dict, _time_str = map_dict1[_timeframe]

                    # update cache dicts
                    if _database_item not in _cache_dict:
                        _cache_dict[_database_item] = self._single_value(_database_item, _time_str)

                    # calculate value
                    delta_value = round(value - _cache_dict[_database_item], 2)

                    # set item value
                    if delta_value != item():
                        item(delta_value, self.get_shortname())

    def execute_due_items(self):
        """
        Ermittlung aller fälligen Funktionen und übergabe an Berechnung
        """

        self.logger.debug("execute_due_items called")

        _todo_items = self._create_due_items()
        self.logger.debug(f"execute_fcts: Following items will be calculated: {_todo_items}")

        self.execute_items(_todo_items)

    def execute_items(self, item_list):
        """
        Übergabe der Items an die entsprechende Berechnungsfunktion
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

            if _database_addon_fct == 'oldest_value':
                _result = self._get_oldest_value(_database_item)

            elif _database_addon_fct == 'oldest_log':
                _result = self._get_oldest_log(_database_item)

            elif _database_addon_fct == 'zaehlerstand_heute':
                _result = _database_item.property.value

            elif _database_addon_fct == 'db_version':
                _result = self._get_db_version()

            elif _database_addon_fct == 'kaeltesumme':
                if self.has_iattr(item.conf, 'database_addon_params'):
                    # _database_addon_params = json.loads(self.get_iattr_value(item.conf, 'database_addon_params'))
                    _database_addon_params = parse_params_to_dict(self.get_iattr_value(item.conf, 'database_addon_params'))
                    _database_addon_params['item'] = _database_item
                    if _database_addon_params.keys() & {'item', 'year'}:
                        _result = self.kaeltesumme(**_database_addon_params)
                    else:
                        self.logger.error(f"Attribute 'database_addon_params' not containing needed params for Item {item.id} with _database_addon_fct={_database_addon_fct}.")
                else:
                    self.logger.error(f"Attribute 'database_addon_params' not given for Item {item.id} with _database_addon_fct={_database_addon_fct}.")

            elif _database_addon_fct == 'waermesumme':
                if self.has_iattr(item.conf, 'database_addon_params'):
                    _database_addon_params = parse_params_to_dict(self.get_iattr_value(item.conf, 'database_addon_params'))
                    _database_addon_params['item'] = _database_item
                    if _database_addon_params.keys() & {'item', 'year'}:
                        _result = self.waermesumme(**_database_addon_params)
                    else:
                        self.logger.error(f"Attribute 'database_addon_params' not containing needed params for Item {item.id} with _database_addon_fct={_database_addon_fct}.")
                else:
                    self.logger.error(f"Attribute 'database_addon_params' not given for Item {item.id} with _database_addon_fct={_database_addon_fct}.")

            elif _database_addon_fct == 'gruendlandtempsumme':
                if self.has_iattr(item.conf, 'database_addon_params'):
                    _database_addon_params = parse_params_to_dict(self.get_iattr_value(item.conf, 'database_addon_params'))
                    _database_addon_params['item'] = _database_item
                    if _database_addon_params.keys() & {'item', 'year'}:
                        _result = self.gts(**_database_addon_params)
                    else:
                        self.logger.error(f"Attribute 'database_addon_params' not containing needed params for Item {item.id} with _database_addon_fct={_database_addon_fct}.")
                else:
                    self.logger.error(f"Attribute 'database_addon_params' not given for Item {item.id} with _database_addon_fct={_database_addon_fct}.")

            # handle all live functions of format 'timeframe_function' like 'heute_max'
            elif len(_var) == 2 and _var[1] in ['min', 'max']:
                self.logger.debug(f"execute_items: live function={_var[0]} with {_var[1] }detected; will be calculated by next update of database item")

            # handle all live functions of format 'timeframe' like 'heute'
            elif len(_var) == 1 and _var[0]:
                self.logger.debug(f"execute_items: live function={_var} detected; will be calculated by next update of database item")

            # handle all functions starting with last in format 'last_window_function' like 'last_24h_max'
            elif len(_var) == 3 and _var[0] == 'last':
                _window = _var[1]
                _func = _var[2]

                self.logger.debug(f"execute_items: 'last' function detected. _window={_window}, _func={_func}")

                if _window[-1:] in ['i', 'h', 'd', 'w', 'm', 'y']:
                    if _window[:-1].isdigit():
                        _result = _database_item.db(_func, _window)

            # handle all functions 'wertehistorie min/max' in format 'timeframe_timedelta_func' like 'heute_minus2_max'
            elif len(_var) == 3 and _var[2] in ['min', 'max']:
                _timeframe = _var[0]
                _timedelta = _var[1][-1]
                if not isinstance(_timedelta, int):
                    _timedelta = int(_timedelta)
                _func = _var[2]

                self.logger.debug(f"execute_items: _database_addon_fct={_func} detected; _timeframe={_timeframe}, _timedelta={_timedelta}")

                _time_str_1, _time_str_2 = self._get_time_strs(_timeframe, _timedelta)
                if _time_str_1 is not None and _time_str_2 is not None:
                    _result = self._value_of_db_function(_database_item, _func, _time_str_1, _time_str_2)

            # handle all functions 'wertehistorie total' in format 'timeframe_timedelta' like 'heute_minus2'
            elif len(_var) == 2 and _var[1].startswith('minus'):
                _timeframe = _var[0]
                _timedelta = _var[1][-1]
                if not isinstance(_timedelta, int):
                    _timedelta = int(_timedelta)

                self.logger.debug(f"execute_items: 'wertehistorie total' function detected. _timeframe={_timeframe}, _timedelta={_timedelta}")

                _time_str_1, _time_str_2 = self._get_time_strs(_timeframe, _timedelta)
                if _time_str_1 is not None and _time_str_1 is not None:
                    _result = self._delta_value(_database_item, _time_str_2, _time_str_1)

            # handle all functions of format 'function_timeframe_timedelta' like 'zaehlerstand_woche_minus1'
            elif len(_var) == 3 and _var[2].startswith('minus'):
                _func = _var[0]
                _timeframe = _var[1]
                _timedelta = _var[2][-1]
                if not isinstance(_timedelta, int):
                    _timedelta = int(_timedelta)

                self.logger.debug(f"execute_items: {_func} function detected. _timeframe={_timeframe}, _timedelta={_timedelta}")

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
                if not isinstance(_timedelta, int):
                    _timedelta = int(_timedelta)

                self.logger.debug(
                    f"execute_items: {_func} function detected. _window={_window}  _timeframe={_timeframe}, _timedelta={_timedelta}")

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

            else:
                self.logger.warning(f"execute_items: No function defined or found")

            self.logger.debug(f"execute_items: result is {_result} for item '{item}' with _database_addon_fct={_database_addon_fct} _database_item={_database_item}")

            # set item value
            if _result is not None:
                item(_result, self.get_shortname())

    def _get_itemid(self, item):

        _item_id = self._itemid_dict.get(item, None)
        if _item_id is None:
            _item_id = self._db_plugin.id(item)
            self._itemid_dict[item] = _item_id

        return _item_id
        
    def _create_due_items(self):
        """
        Ermittlung der zum Ausführungszeitpunkt fälligen Berechnungen
        Leeren der dicts zur Zwischenspeicherung

        Wird täglich per Scheduler/Crontab getriggert und ermittelt basierend auf der Systemtzeit, welche Funktionen ausgeführt werden müssen
        
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

    def _check_db_existance(self):
        """
        Checks if DB Plugin is loaded and if driver is PyMySql
        Gets database plugin parameters
        Does connection test
        Puts database connection parameters to plugin properties
        """

        # check if database plugin is loaded
        try:
            _db_plugin = self.plugins.return_plugin('database')
        except Exception as e:
            self.logger.error(f"Database plugin not loaded, Error was {e}. No need for DatabaseAddOn Plugin.")
            return False

        # get driver of database and check if it is PyMySql to ensure existence of MySql DB
        try:
            db_driver = _db_plugin.get_parameter_value('driver')
        except Exception as e:
            self.logger.error(f"Error {e} occured during getting database plugin parameter 'driver'. DatabaseAddOn Plugin not loaded.")
            return False
        else:
            if db_driver.lower() != 'pymysql':
                self.logger.error(
                    f"Database plugin not loaded, but driver ist not 'PyMySql'. DatabaseAddOn Plugin not loaded.")
                return False

        # get database plugin parameters
        try:
            db_instance = _db_plugin.get_parameter_value('instance')
            connection_data = _db_plugin.get_parameter_value('connect')  # ['host:localhost', 'user:smarthome', 'passwd:smarthome', 'db:smarthome', 'port:3306']
            self.logger.debug(f"Database Plugin available with instance={db_instance} and connection={connection_data}")
        except Exception as e:
            self.logger.error(f"Error {e} occured during getting database plugin parameters. DatabaseAddOn Plugin not loaded.")
            return False
        else:
            try:
                host = connection_data[0].split(':', 1)[1]
                user = connection_data[1].split(':', 1)[1]
                password = connection_data[2].split(':', 1)[1]
                db = connection_data[3].split(':', 1)[1]
                # port = connection_data[4].split(':', 1)[1]
            except Exception as e:
                self.logger.error(f"Not able to get Database parameters, Error was {e}. DatabaseAddOn Plugin not loaded.")
                return False

        # do connection check
        try:
            _db_connection = self._connect_to_db(host=host, user=user, password=password, db=db)
        except Exception as e:
            self.logger.error(f"Connection to mysql database failed with error {e}. DatabaseAddOn Plugin not loaded.")
            return False

        if _db_connection:
            self._db_plugin = _db_plugin
            self.logger.debug(f"Connection check to mysql database successfull.")
            _db_connection.close()
            self._db_host = host
            self._db_user = user
            self._db_pw = password
            self._db_db = db
            return True
        else:
            self.logger.error(f"Connection to mysql database not possible. DatabaseAddOn Plugin not loaded.")
            return False

    def _connect_to_db(self, host=None, user=None, password=None, db=None):
        """
        Connect to DB via pymysql
        """
        host = self._db_host if not host else host
        user = self._db_user if not user else user
        password = self._db_pw if not password else password
        db = self._db_db if not db else db

        try:
            connection = pymysql.connect(host=host, user=user, password=password, db=db, charset='utf8mb4', cursorclass=pymysql.cursors.DictCursor)
        except Exception as e:
            self.logger.error(f"Connection to Database failed with error {e}!.")
            return
        else:
            return connection

    def _get_oldest_log(self, item):
        """
        Ermittlung des Zeitpunktes des ältesten Eintrags eines Items in der DB

        :param item: Item, für das der älteste Eintrag ermittelt werden soll
        :return: timestamp des ältesten Eintrags für das Item aus der DB
        """

        # Zwischenspeicher des oldest_log, zur Reduktion der DB Zugriffe
        if item in self._oldest_log_dict:
            oldest_log = self._oldest_log_dict[item]
        else:
            item_id = self._get_itemid(item)
            oldest_log = self._db_plugin.readOldestLog(item_id)
            self._oldest_log_dict[item] = oldest_log

        self.logger.debug(f"_get_oldest_log for item {item.id()} = {oldest_log}")
        return oldest_log

    def _get_oldest_value(self, item):
        """
        Ermittlung des ältesten Wertes eines Items in der DB

        :param item: Item, für das der älteste Wert ermittelt werden soll
        :return: ältester Wert für das Item aus der DB oder None bei Fehler
        """

        if item in self._oldest_entry_dict:
            oldest_entry = self._oldest_entry_dict[item]
        else:
            item_id = self._get_itemid(item)
            oldest_entry = self._db_plugin.readLog(item_id, self._get_oldest_log(item))
            self._oldest_entry_dict[item] = oldest_entry

        self.logger.debug(f"_get_oldest_value for item {item.id()} = {self._oldest_entry_dict[item][0][4]}")
        return oldest_entry[0][4]

    def _time_since_oldest_log(self, item):
        """
        Ermittlung der Zeit in ganzen Minuten zwischen "now" und dem ältesten Eintrag eines Items in der DB

        :param item: Item, für das die Zeit seit dem ältesten Eintrag ermittelt werden soll
        :return: Zeit seit dem ältesten Eintrag in der DB in ganzen Minuten
        """

        _timestamp = self._get_oldest_log(item)
        _oldest_log_dt = datetime.datetime.fromtimestamp(int(_timestamp) / 1000, datetime.timezone.utc).astimezone().strftime('%Y-%m-%d %H:%M:%S %Z%z')
        return self.shtime.time_since(_oldest_log_dt, resulttype='im')

    def _delta_value(self, item, time_str_1, time_str_2):
        """
        Berechnung einer Zählerdifferenz eines Items zwischen 2 Zeitpunkten auf Basis der DB Einträge

        :param item: Item, für das die Zählerdifferenz ermittelt werden soll
        :param time_str_1: Zeitstring gemäß database-Plugin für den jüngeren Eintrag (bspw: 200i)
        :param time_str_2: Zeitstring gemäß database-Plugin für den älteren Eintrag (bspw: 400i)
        """

        # value_1 = value_2 = value = None
        time_since_oldest_log = self._time_since_oldest_log(item)
        end = int(time_str_1[0:len(time_str_1) - 1])

        if time_since_oldest_log > end:
            self.logger.debug(f'_delta_value: fetch DB with {item.id()}.db(max, {time_str_1}, {time_str_1})')
            value_1 = item.db('max', time_str_1, time_str_1)
            self.logger.debug(f'_delta_value: fetch DB with {item.id()}.db(max, {time_str_2}, {time_str_2})')
            value_2 = item.db('max', time_str_2, time_str_2)
            if value_1 is not None:
                if value_2 is None:
                    self.logger.info(f'No entries for Item {item.id()} in DB found for requested enddate {time_str_1}; try to use oldest entry instead')
                    value_2 = self._oldest_value(item)
                if value_2 is not None:
                    value = round(value_1 - value_2, 2)
                    self.logger.debug(f'_delta_value for item={item.id()} with time_str_1={time_str_1} and time_str_2={time_str_2} is {value}')
                    return value
        else:
            self.logger.debug(f'_delta_value for item={item.id()} using time_str_1={time_str_1} is older as oldest_entry. Therefore no DB request initiated.')

    def _single_value(self, item, time_str_1, func='max'):
        """
        Abfrage der DB nach dem Wert eines Items zum entsprechenden Zeitpunkt

        :param item: Item, für das der DB-Wert ermittelt werden soll
        :param time_str_1: Zeitstring gemäß database-Plugin für den Abfragezeitpunkt (bspw: 200i)
        :param func: DB function
        """

        # value = None
        value = item.db(func, time_str_1, time_str_1)
        if value is None:
            self.logger.info(f'No entries for Item {item.id()} in DB found for requested end {time_str_1}; try to use oldest entry instead')
            value = int(self._oldest_value(item))
        self.logger.debug(f'_single_value for item={item.id()} with time_str_1={time_str_1} is {value}')
        return value

    def _value_of_db_function(self, item, func, time_str_1, time_str_2):
        """
        Abfrage der DB mit einer DB-Funktion und Start und Ende des Betrachtungszeitraumes

        :param item: Item, für das die Zählerdifferenz ermittelt werden soll
        :param func: Funktion, mit der die Zählerdifferenz ermittelt werden soll (bspw max)
        :param time_str_1: Zeitstring gemäß database-Plugin für den Start es Betrachtungszeitraumes (bspw: 400i)
        :param time_str_2: Zeitstring gemäß database-Plugin für das Ende des Betrachtungszeitraumes (bspw: 200i)
        """
        self.logger.debug(f"_value_of_db_function called with item={item.id()}, func={func}, time_str_1={time_str_1} and time_str_2={time_str_2}")
        value = item.db(func, time_str_1, time_str_2)
        if value is None:
            self.logger.info(f'_value_of_db_function: No entries for Item {item} in DB found for requested startdate {time_str_1}; try to use oldest entry instead')
            time_since_oldest_log = self._time_since_oldest_log(self._oldest_log(item))
            end = int(time_str_2[0:len(time_str_2) - 1])
            if time_since_oldest_log > end:
                time_str_1 = f"{self._time_since_oldest_log(self._oldest_log(item))}i"
                value = item.db(func, time_str_1, time_str_2)
            else:
                self.logger.info(f"_value_of_db_function for item={item.id()}: 'time_since_oldest_log' <= 'end'")
        self.logger.debug(f'_value_of_db_function for item={item.id()} with function={func}, time_str_1={time_str_1}, time_str_2={time_str_2} is {value}')
        return value

    def _get_time_strs(self, key, x):

        if 'heute' in key:
            _time_str_1 = self._time_str_heute_minus_x(x)
            _time_str_2 = self._time_str_heute_minus_x(x - 1)
        elif 'woche' in key:
            _time_str_1 = self._time_str_woche_minus_x(x)
            _time_str_2 = self._time_str_woche_minus_x(x - 1)
        elif 'monat' in key:
            _time_str_1 = self._time_str_monat_minus_x(x)
            _time_str_2 = self._time_str_monat_minus_x(x - 1)
        elif 'jahr' in key:
            _time_str_1 = self._time_str_jahr_minus_x(x)
            _time_str_2 = self._time_str_jahr_minus_x(x - 1)
        else:
            _time_str_1 = None
            _time_str_2 = None
        # self.logger.debug(f"_time_str_1={_time_str_1}, _time_str_2={_time_str_2}")
        return _time_str_1, _time_str_2

    def _time_str_heute_minus_x(self, x=0):
        """creates an str for db request in min from now x days ago"""
        return f"{self.shtime.time_since(self.shtime.today(-x), 'im')}i"

    def _time_str_woche_minus_x(self, x=0):
        """creates an str for db request in min from now x weeks ago"""
        return f"{self.shtime.time_since(self.shtime.beginning_of_week(self.shtime.calendar_week(), None, -x), 'im')}i"

    def _time_str_monat_minus_x(self, x=0):
        """creates an str for db request in min from now x month ago"""
        return f"{self.shtime.time_since(self.shtime.beginning_of_month(None, None, -x), 'im')}i"

    def _time_str_jahr_minus_x(self, x=0):
        """creates an str for db request in min from now x month ago"""
        return f"{self.shtime.time_since(self.shtime.beginning_of_year(None, -x), 'im')}i"

    def _get_dbtimestamp_from_date(self, date):
        if type(date) is datetime.date:
            d = date
        else:
            date = date.split('-', 1)
            year = int(date[0])
            month = int(date[1])
            d = datetime.date(year, month, 1)
        return int(time.mktime(d.timetuple()) * 1000)

    def _read_item_table(self, item):
        """
        :param item: name or Item_id of the item within the database
        :return: Data for the selected item
        """
        columns_entries = ('id', 'name', 'time', 'val_str', 'val_num', 'val_bool', 'changed')
        columns = ", ".join(columns_entries)

        if isinstance(item, Item):
            query = f"SELECT {columns} FROM item WHERE name = '{str(item.id())}'"
            return self._execute_query_one(query)
        else:
            try:
                item = int(item)
            except:
                pass
            else:
                query = f"SELECT {columns} FROM item WHERE id = {item}"
                return self._execute_query_one(query)

    def _get_item_id(self, item):
        """
        Returns the ID of the given item

        :param item: Item to get the ID for
        :return: id of the item within the database
        :rtype: int | None
        """
        self.logger.debug(f"'_get_item_id' has been called with item={item.id()}")

        _item_id = self._itemid_dict.get(item, None)
        if _item_id is None:
            row = self._read_item_table(item)
            if row:
                if len(row) > 0:
                    _item_id = int(row['id'])
                    self._itemid_dict[item] = _item_id
        return _item_id

    def _fetch_all(self, item):

        self.logger.debug(f"'_fetch_all' has been called for item={item}")
        if isinstance(item, Item):
            item_id = self._get_item_id(item)
        else:
            item_id = int(item)
        if not item_id:
            return

        query = "select * from log where (item_id=%s) AND (time = None OR 1 = 1)"
        param_dict = {'item_id': item_id}

        result = self._execute_query(query, param_dict)
        return result

    def _get_db_version(self):

        self.logger.debug(f"'_get_db_version' has been called")
        connection = self._connect_to_db()
        if connection:
            try:
                with connection.cursor() as cursor:
                    cursor.execute('SELECT VERSION()')
                    result = cursor.fetchone()
            except Exception as e:
                self.logger.error(f"_get_db_version failed with error={e}")
            else:
                self.logger.debug(f'_get_db_version result={result}')
                return list(result.values())[0]
            finally:
                connection.close()

    def fetch_log(self, func, item, timespan, start=None, end=0, count=None, group=None, group2=None):
        """
        """
        self.logger.debug(f"'fetch_log' has been called with func={func}, item={item.id()}, timespan={timespan}, start={start}, end={end}, count={count}, group={group}")

        if start is None and count is not None:
            start = int(end) + int(count)
            if not start:
                return
        self.logger.debug(f"fetch_log: item={item.id()} of type={type(item)}")

        if isinstance(item, Item):
            item_id = self._get_item_id(item)
        else:
            item_id = int(item)
        if not item_id:
            return

        param_dict = {
            'item': item_id,
            'end': int(end),
            'start': int(start)
            }

        select = {
            'avg': 'UNIX_TIMESTAMP(DATE(FROM_UNIXTIME(time/1000))) * 1000 as time1, ROUND(AVG(val_num * duration) / AVG(duration), 1) as value',
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

        if start is None and end == 0:
            where = {
                    'month': 'item_id = %(item)s',
                    'week': 'item_id = %(item)s',
                    'day': 'item_id = %(item)s'
                    }
        else:  # Abfrage von heute - x (Anzahl) Tage/Wochen/Monate bis y (Anzahl) Tage/Wochen/Monate zurück
            where = {
                    'year': 'item_id = %(item)s AND DATE(FROM_UNIXTIME(time/1000)) BETWEEN MAKEDATE(year(now()-interval %(start)s year),1) AND DATE_SUB(CURRENT_DATE, INTERVAL %(end)s YEAR)',
                    'month': 'item_id = %(item)s AND DATE(FROM_UNIXTIME(time/1000)) BETWEEN DATE_SUB(DATE_ADD(MAKEDATE(YEAR(CURRENT_DATE), 1), INTERVAL MONTH(CURRENT_DATE)-1 MONTH), INTERVAL %(start)s MONTH) AND DATE_SUB(CURRENT_DATE, INTERVAL %(end)s MONTH)',
                    'week': 'item_id = %(item)s AND DATE(FROM_UNIXTIME(time/1000)) BETWEEN DATE_SUB(DATE_ADD(CURRENT_DATE, INTERVAL -WEEKDAY(CURRENT_DATE) DAY), INTERVAL %(start)s WEEK) AND DATE_SUB(CURRENT_DATE, INTERVAL %(end)s WEEK)',
                    'day': 'item_id = %(item)s AND DATE(FROM_UNIXTIME(time/1000)) BETWEEN DATE_SUB(CURDATE(), INTERVAL %(start)s DAY) AND DATE_SUB(CURDATE(), INTERVAL %(end)s DAY)'
                    }

        group_by = {
                    'year': 'GROUP BY YEAR(FROM_UNIXTIME(time/1000))',
                    'month': 'GROUP BY YEAR(FROM_UNIXTIME(time/1000)), MONTH(FROM_UNIXTIME(time/1000))',
                    'week': 'GROUP BY YEARWEEK(FROM_UNIXTIME(time/1000), 5)',
                    'day': 'GROUP BY DATE(FROM_UNIXTIME(time/1000))',
                    None: ''
                    }

        table_alias = {
                    'avg': '',
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

        if timespan not in where.keys():
            return ['Requested time increment not defined; Need to be year, month, week, day']
        if func not in select.keys():
            return ['Requested function is not defined.']

        query = f"SELECT {select[func]} FROM log WHERE {where[timespan]} {group_by[group]} ORDER BY time ASC {table_alias[func]} {group_by[group2]}"

        self.logger.debug(f"fetch_log: query={query}, param_dict={param_dict}")

        result = self._execute_query(query, param_dict)
        value = []
        for element in result:
            value.append([element['time1'], element['value']])
        if func == 'diff_max':
            value.pop(0)
        self.logger.debug(f"fetch_log value for item={item} with timespan={timespan}, func={func}: {value}")
        return value

    def _execute_query(self, query, param_dict=None):

        self.logger.debug(f"'_execute_query' has been called with query={query}, param_dict={param_dict}")
        connection = self._connect_to_db()
        if connection:
            try:
                with connection.cursor() as cursor:
                    cursor.execute(query, param_dict)
                    result = cursor.fetchall()
            except Exception as e:
                self.logger.error(f"_execute_query failed with error={e}")
            else:
                self.logger.debug(f'_execute_query result={result}')
                return result
            finally:
                connection.close()

    def _execute_query_one(self, query, param_dict=None):

        self.logger.debug(f"'_execute_query_one' has been called with query={query}, param_dict={param_dict}")
        connection = self._connect_to_db()
        if connection:
            try:
                with connection.cursor() as cursor:
                    cursor.execute(query, param_dict)
                    result = cursor.fetchone()
            except Exception as e:
                self.logger.error(f"_execute_query_one failed with error={e}")
            else:
                self.logger.debug(f'_execute_query_one result={result}')
                return result
            finally:
                connection.close()

    def _execute_query_w_commit(self, query, param_dict=None):

        self.logger.debug(f"'_execute_query_w_commit' has been called with query={query}, param_dict={param_dict}")
        connection = self._connect_to_db()
        if connection:
            try:
                with connection.cursor() as cursor:
                    cursor.execute(query, param_dict)
                connection.commit()
            except Exception as e:
                self.logger.error(f"_execute_query_w_commit failed with error={e}")
            finally:
                connection.close()

    def gts(self, item, year):
        """Calculates the Grünlandtemperatursumme for given item and year"""

        year = int(year)
        current_year = datetime.date.today().year
        year_delta = current_year - year
        result = self.fetch_log(func='max', item=item, timespan='year', start=year_delta, end=year_delta, group='day')
        gts = 0
        for entry in result:
            dt = datetime.datetime.fromtimestamp(entry[0] / 1000)
            if dt.month == 1:
                gts += entry[1] * 0.5
            elif dt.month == 2:
                gts += entry[1] * 0.75
            else:
                gts += entry[1]
        return int(round(gts, 0))

    def waermesumme(self, item, year, month=None):
        """Calculates the Wärmesumme for given item, year and month"""

        if month is None:
            start_date = datetime.date(int(year), 3, 20)
            end_date = datetime.date(int(year), 9, 21)
            group2 = 'year'
        else:
            start_date = datetime.date(int(year), int(month), 1)
            end_date = start_date + relativedelta(months=+1) - datetime.timedelta(days=1)
            group2 = 'month'

        today = datetime.date.today()
        start = (today - start_date).days
        end = (today - end_date).days if end_date < today else 0

        result = self.fetch_log(func='sum_max', item=item, timespan='day', start=start, end=end, group='day', group2=group2)
        if result:
            if month is None:
                result = result[0][1]
            return result

    def kaeltesumme(self, item, year, month=None):
        """Calculates the Kältesumme for given item, year and month"""

        if month is None:
            start_date = datetime.date(int(year), 9, 21)
            end_date = datetime.date(int(year) + 1, 3, 22)
            group2 = None
        else:
            start_date = datetime.date(int(year), int(month), 1)
            end_date = start_date + relativedelta(months=+1) - datetime.timedelta(days=1)
            group2 = 'month'

        today = datetime.date.today()
        start = (today - start_date).days
        end = (today - end_date).days if end_date < today else 0

        result = self.fetch_log(func='sum_min_neg', item=item, timespan='day', start=start, end=end, group='day', group2=group2)
        if result:
            if month is None:
                result = result[0][1]
            return result


def parse_params_to_dict(string):
    """parse a string with named arguments and comma sparation to dict; string = 'year=2022, month=12'"""

    try:
        res_dict = dict((a.strip(), b.strip())
        for a, b in (element.split('=')
            for element in string.split(', ')))
    except:
        return None
    else:
        return res_dict


##############################
##### Backup
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
