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
from lib.shtime import Shtime
from lib.plugin import Plugins
from .webif import WebInterface

import pymysql.cursors
import datetime


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
        self._static_items = set()                  #
        self._itemid_dict = {}
        self._oldest_log_dict = {}
        self._oldest_entry_dict = {}
        self._todo_items = set()
        self._todo_tasks = []
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
                self.logger.debug(f"Item '{item.id()}' added with database_addon_fct={_database_addon_fct} and database_item={_database_item}")
                self._item_dict[item] = (_database_addon_fct, _database_item)
                
                # add item to set of items for time of execution
                if _database_addon_fct.startswith('zaehlerstand'):
                    self._meter_items.add(item)
                elif 'heute_minus' in _database_addon_fct:
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
            _database_item = self._item_dict[item][1]
            _time_str_1 = None
            _time_str_2 = None
            _result = None

            self.logger.debug(f"execute_items: item '{item}' is due with _database_addon_fct={_database_addon_fct} _database_item={_database_item}")

            if _database_addon_fct == 'oldest_value':
                _result = self._get_oldest_value(_database_item)

            elif _database_addon_fct == 'oldest_log':
                _result = self._get_oldest_log(_database_item)

            elif _database_addon_fct == 'zaehlerstand_heute':
                _result = _database_item.property.value

            # get all functions ending of _max, _min, _avg
            elif _database_addon_fct[-3:] in ['max', 'min', 'avg']:
                left, sep, func = _database_addon_fct.rpartition('_')
                self.logger.debug(f"execute_items: _database_addon_fct={func} detected; left={left}, last_char={left[-1]}")
                last_char = left[-1]
                try:
                    x = int(last_char)
                except:
                    pass
                else:
                    _time_str_1, _time_str_2 = self._get_time_strs(left, x)
                    if _time_str_1 is not None and _time_str_2 is not None:
                        _result = self._value_of_db_function(_database_item, func, _time_str_1, _time_str_2)

            # get all wertehistorie total functions
            elif _database_addon_fct[:-1].endswith('minus'):
                self.logger.debug(f"execute_items: normal 'wertehistorie total' function detected")
                last_char = _database_addon_fct[-1]
                try:
                    x = int(last_char)
                except:
                    pass
                else:
                    if _database_addon_fct.startswith('zaehlerstand_'):
                        _time_str_1, _time_str_2 = self._get_time_strs(_database_addon_fct, x)
                        if _time_str_1 is not None:
                            _result = self._single_value(_database_item, _time_str_1)
                    elif _database_addon_fct.startswith('rolling_'):
                        if 'woche' in _database_addon_fct:
                            _time_str_1 = self._time_str_heute_minus_x(0)
                            _time_str_2 = self._time_str_heute_minus_x(365)
                        elif 'monat' in _database_addon_fct:
                            _time_str_1 = self._time_str_monat_minus_x(0)
                            _time_str_2 = self._time_str_monat_minus_x(12)
                        elif 'jahr' in _database_addon_fct:
                            _time_str_1 = self._time_str_jahr_minus_x(0)
                            _time_str_2 = self._time_str_jahr_minus_x(1)

                        if _time_str_1 is not None and _time_str_2 is not None:
                            value = self._delta_value(_database_item, _time_str_1, _time_str_2)
                    else:
                        _time_str_1, _time_str_2 = self._get_time_strs(_database_addon_fct, x)
                        if _time_str_1 is not None and _time_str_1 is not None:
                            _result = self._delta_value(_database_item, _time_str_2, _time_str_1)
            else:
                _result = 'No function defined or found'

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

        Wird täglich per Scheduler/Crontab getriggert und ermittelt basierend auf der Systemtzeit, welche Funktionen ausgeführt werden müssen
        
        """
        
        _todo_items = set()
        # täglich zu berechnende Items werden täglich berechnet
        _todo_items.update(self._daily_items)
        # wenn Wochentag = Montag, werden auch die wöchentlichen Items berechnet
        if self.shtime.now().hour == 0 and self.shtime.now().minute == 0 and self.shtime.weekday(self.shtime.today()) == 1:
            _todo_items.update(self._weekly_items)
        # wenn erster Tage (des Monates), werden auch die monatlichen Items berechnet
        if self.shtime.now().hour == 0 and self.shtime.now().minute == 0 and self.shtime.now().day == 1:
            _todo_items.update(self._montly_items)
        # wenn erster Tage des ersten Monates, werden auch die jährlichen Items berechnet
        if self.shtime.now().hour == 0 and self.shtime.now().minute == 0 and self.shtime.now().day == 1 and self.shtime.now().month == 1:
            _todo_items.update(self._yearly_items)
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
            _db_connection = self._connect_to_db(host, user, password, db, charset='utf8mb4')
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

    def _connect_to_db(self, host, user, password, db, charset):
        """
        Connect to DB via pymysql
        """

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
        if 'heute_' in key:
            _time_str_1 = self._time_str_heute_minus_x(x)
            _time_str_2 = self._time_str_heute_minus_x(x - 1)
        elif 'woche_' in key:
            _time_str_1 = self._time_str_woche_minus_x(x)
            _time_str_2 = self._time_str_woche_minus_x(x - 1)
        elif 'monat_' in key:
            _time_str_1 = self._time_str_monat_minus_x(x)
            _time_str_2 = self._time_str_monat_minus_x(x - 1)
        elif 'jahr_' in key:
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
