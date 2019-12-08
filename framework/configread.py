# -*- coding: utf-8 -*-
import configparser
from configparser import ExtendedInterpolation
from definitions import ROOT_DIR


class ReadConfig(object):
    def __init__(self, config_name=None):
        self.config = configparser.ConfigParser(interpolation=ExtendedInterpolation())
        self.config_name = [config_name, 'config.dev.ini'][config_name is None]
        self.config.read('%s/%s' % (ROOT_DIR, self.config_name))

    def option(self, section, option):
        """

        :param section: name of config section
        :param option: name of option of the section
        :return: None
        """

        if self.config.has_option(section, option):
            return self.config.get(section, option)

    def section(self, section):
        """

        :param section: name of config section
        :return: None
        """
        if self.config.has_section(section):
            return self.config[section]