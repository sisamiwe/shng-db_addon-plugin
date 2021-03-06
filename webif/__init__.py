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

import json
import cherrypy
from lib.item import Items
from lib.model.smartplugin import SmartPluginWebIf
from jinja2 import Environment, FileSystemLoader


class WebInterface(SmartPluginWebIf):

    def __init__(self, webif_dir, plugin):
        """
        Initialization of instance of class WebInterface

        :param webif_dir: directory where the webinterface of the plugin resides
        :param plugin: instance of the plugin
        :type webif_dir: str
        :type plugin: object
        """
        self.logger = plugin.logger
        self.webif_dir = webif_dir
        self.plugin = plugin
        self.items = Items.get_instance()
        self.tplenv = self.init_template_environment()

    @cherrypy.expose
    def index(self, reload=None):
        """
        Build index.html for cherrypy

        Render the template and return the html file to be delivered to the browser

        :return: contents of the template after beeing rendered
        """

        # get webif_pagelength
        try:
            pagelength = self.plugin.webif_pagelength
        except Exception:
            pagelength = 100

        maintenance = True if self.plugin.get_log_level <= 20 else False

        tmpl = self.tplenv.get_template('index.html')

        return tmpl.render(p=self.plugin,
                           webif_pagelength=pagelength,
                           items=sorted(self.plugin.item_list, key=lambda k: str.lower(k['_path'])),
                           item_count=len(self.plugin.item_list),
                           plugin_shortname=self.plugin.get_shortname(),
                           plugin_version=self.plugin.get_version(),
                           plugin_info=self.plugin.get_info(),
                           maintenance=maintenance,
                           )

    @cherrypy.expose
    def get_data_html(self, dataSet=None):
        """
        Return data to update the webpage

        For the standard update mechanism of the web interface, the dataSet to return the data for is None

        :param dataSet: Dataset for which the data should be returned (standard: None)
        :return: dict with the data needed to update the web page.
        """
        if dataSet is None:
            # get the new data
            data = self.plugin._webdata
            for item in self.plugin._item_dict:
                if data.get(item.id(), None):
                    data[item.id()]['last_update'] = item.property.last_update.strftime('%d.%m.%Y %H:%M:%S')
                    data[item.id()]['last_change'] = item.property.last_change.strftime('%d.%m.%Y %H:%M:%S')
            try:
                return json.dumps(data, default=str)
            except Exception as e:
                self.logger.error(f"get_data_html exception: {e}")
        return {}

    @cherrypy.expose
    def recalc_all(self):
        self.logger.debug(f"recalc_all called")
        self.plugin.execute_all_items()

    @cherrypy.expose
    def clean_cache_dicts(self):
        self.logger.debug(f"_clean_cache_dicts called")
        self.plugin._clean_cache_dicts()
