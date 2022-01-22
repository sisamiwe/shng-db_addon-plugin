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
        self._item_itemid = set()                   # set of tuples for [(item, itemid1), (item2, itemid2), ...]
        self._daily_items = set()                   # set of items, for which the _database_addon_fct shall be executed daily
        self._weekly_items = set()                  # set of items, for which the _database_addon_fct shall be executed weekly
        self._monthly_items = set()                 # set of items, for which the _database_addon_fct shall be executed monthly
        self._yearly_items = set()                  # set of items, for which the _database_addon_fct shall be executed yearly
        self._db_plugin = None
        self._itemid_dict = {}
        self._oldest_log_dict = {}
        self._oldest_entry_dict = {}
        self._todo_items = set()
        self._todo_tasks = []
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

        self.execute_items(list(self._item_dict.keys()))

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

            if _database_addon_fct and _database_item is not None:
                # add item to item dict
                self.logger.debug(f"Item '{item.id()}' added with database_addon_fct={_database_addon_fct} and database_item={_database_item}")
                self._item_dict[item] = (_database_addon_fct, _database_item)
                
                # add item to set of items for time of execution
                if _database_addon_fct in wertehistorie_total_daily:
                    self._daily_items.add(item)
                elif _database_addon_fct in wertehistorie_total_weekly:
                    self._weekly_items.add(item)
                elif _database_addon_fct in wertehistorie_total_monthly:
                    self._monthly_items.add(item)
                elif _database_addon_fct in wertehistorie_total_yearly:
                    self._yearly_items.add(item)

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

            self.logger.debug(f"item '{item}' is due with _database_addon_fct={_database_addon_fct} _database_item={_database_item}")

            if _database_addon_fct == 'oldest_value':
                _result = self._get_oldest_value(_database_item)

            elif _database_addon_fct == 'oldest_log':
                _result = self._get_oldest_log(_database_item)

            else:
                _result = 'No function defined or found'

            self.logger.debug(f"result is {_result} for item '{item}' with _database_addon_fct={_database_addon_fct} _database_item={_database_item}")

    def _get_itemid_from_item(self, item):
        """ get item_id from am item; uses list of tuples for [(device1, uuid1), (device2, uuid2), ...]"""

        return dict(self._item_itemid).get(item, None)
        
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
        except:
            self.logger.error(f"Database plugin not loaded; No need for DatabaseAddOn Plugin.")
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
                self.logger.error(f"Not able to get Database parameters. DatabaseAddOn Plugin not loaded.")
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

    def _get_oldest_log(self, item, item_id=None):
        """
        Ermittlung des Zeitpunktes des ältesten Eintrags eines Items in der DB

        :param item: Item, für das der älteste Eintrag ermittelt werden soll
        :return: timestamp des ältesten Eintrags für das Item aus der DB
        """

        if item_id is None:
            if item in self._itemid_dict:
                item_id = self._itemid_dict[item]
            else:
                item_id = self._db_plugin.id(item)

        # Zwischenspeicher des oldest_log, zur Reduktion der DB Zugriffe
        if item not in self._oldest_log_dict:
            oldest_log = self._db_plugin.readOldestLog(item_id)
            self._oldest_log_dict[item] = oldest_log
        else:
            oldest_log = self._oldest_log_dict[item]

        self.logger.debug(f"_get_oldest_log for item {item.id()} = {oldest_log}")
        return oldest_log

    def _get_oldest_value(self, item):
        """
        Ermittlung des ältesten Wertes eines Items in der DB

        :param item: Item, für das der älteste Wert ermittelt werden soll
        :return: ältester Wert für das Item aus der DB oder None bei Fehler
        """

        if item not in self._itemid_dict:
            item_id = self._db_plugin.id(item)
            self._itemid_dict[item] = item_id
        else:
            item_id = self._itemid_dict[item]
        self.logger.debug(f'itemid_dict: {self._itemid_dict}')

        if item not in self._oldest_entry_dict:
            oldest_entry = self._db_plugin.readLog(item_id, self._get_oldest_log(item, item_id))
            if len(oldest_entry) == 1:
                if len(oldest_entry[0]) == 7:
                    self._oldest_entry_dict[item] = oldest_entry
                else:
                    return
            else:
                return
        else:
            oldest_entry = self._oldest_entry_dict[item]

        self.logger.debug(f"_get_oldest_value for item {item.id()} = {oldest_entry[0][4]}")
        return oldest_entry[0][4]

    def _time_since_oldest_log(self, item):
        """
        Ermittlung der Zeit in ganzen Minuten zwischen "now" und dem ältesten Eintrag eines Items in der DB

        :param item: Item, für das die Zeit seit dem ältesten Eintrag ermittelt werden soll
        :return: Zeit seit dem ältesten Eintrag in der DB in ganzen Minuten
        """

        timestamp = self._oldest_log(item)
        oldest_log_dt = datetime.datetime.fromtimestamp(int(timestamp) / 1000,datetime.timezone.utc).astimezone().strftime('%Y-%m-%d %H:%M:%S %Z%z')
        time_since_oldest_log = self.shtime.time_since(oldest_log_dt, resulttype='im')
        return time_since_oldest_log

    def _time_str_heute_minus_x(self, x=0):
        """creates an str for db request in min from now x days ago"""
        return f"{self.shtime.time_since(self.shtime.today(-x), 'im')}i"

    def _time_str_vorwoche_minus_x(self, x=0):
        """creates an str for db request in min from now x weeks ago"""
        return f"{self.shtime.time_since(self.shtime.beginning_of_week(self.shtime.calendar_week(), None, -x), 'im')}i"

    def _time_str_vormonat_minus_x(self, x=0):
        """creates an str for db request in min from now x month ago"""
        return f"{self.shtime.time_since(self.shtime.beginning_of_month(None, None, -x), 'im')}i"

    def _time_str_vorjahr_minus_x(self, x=0):
        """creates an str for db request in min from now x month ago"""
        return f"{self.shtime.time_since(self.shtime.beginning_of_year(None, -x), 'im')}i"


# define global defaults
wertehistorie_total_live =     ['heute', 'woche', 'monat', 'jahr']
wertehistorie_total_daily =    ['gestern', 'gestern_minus1', 'gestern_minus2', 'gestern_minus3', 'gestern_minus4', 'gestern_minus5', 'gestern_minus6']
wertehistorie_total_weekly =   ['vorwoche', 'vorwoche_minus1', 'vorwoche_minus2', 'vorwoche_minus3']
wertehistorie_total_monthly =  ['vormonat', 'vormonat_minus1', 'vormonat_minus2', 'vormonat_minus3', 'vormonat_minus12']
wertehistorie_total_yearly =   ['vorjahr', 'vorjahr_minus1']
wertehistorie_total_endofday = ['zaehlerstand_tagesende', 'vormonat_zaehlerstand', 'vormonat_minus1_zaehlerstand', 'vormonat_minus2_zaehlerstand']
