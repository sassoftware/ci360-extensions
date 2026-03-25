#Copyright © 2025, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
#SPDX-License-Identifier: Apache-2.0

import configparser
from pathlib import Path
from common.appCommon import AppCommon
import sys
class ApplicationConfiguration:
    __class_initialized = False
    @classmethod
    def initializeConfiguration(cls, ini_path: str="config.ini"):
        configValues:dict = dict()
        if AppCommon.fileExists(ini_path):
            AppCommon.appconfig = configparser.RawConfigParser()
            AppCommon.appconfig.read(ini_path)
            #GCIDCommon.appMessage(f"Reading Configuration File: '{vAppName}.ini'","TRACE")
            for secKey,secValue in AppCommon.appconfig.items():
                for key,value in secValue.items():
                    configValues[key]=value
            for item in configValues:
                setattr(cls, item, ApplicationConfiguration._cast_value(configValues[item]))
                #GCIDCommon.appMessage(f"Config Value: {item} ==> {configValues[item]} ","TRACE")
            #for item in vClass.__dict__:
            #    print(f"{item} : {vClass.__dict__[item]}")
        else:
            raise RuntimeError(f"Application Configuration file '{ini_path}' not found. Exiting...")
            sys.exit(1)
        cls.__class_initialized = True  
        return

    # _config = configparser.ConfigParser()
    # _config.read(ini_path)
    # for section in _config.sections():
    #     section_attrs = {}
    #     for key, value in _config.items(section):
    #         section_attrs[key] = ApplicationConfiguration._cast_value(value)
    #     # Set as class-level attribute
    #     setattr(section,type(section, (), section_attrs)())


    @classmethod
    def _cast_value(cls,value):
        # Try to cast to bool, int, or float, else keep as string
        if value.lower() in ("true", "false"):
            return value.lower() == "true"
        try:
            return int(value)
        except ValueError:
            pass
        try:
            return float(value)
        except ValueError:
            pass
        return value

    @classmethod
    def get(cls,attrib: str, default=None):
        """
        Return the value of a class-level attribute (section or section.option).
        If only section is provided, returns the section object.
        If both section and option are provided, returns the option value or default.
        """
        if not cls.__class_initialized:
            try:
                cls.initializeConfiguration()
            except Exception as e:   
                raise RuntimeError(f"ApplicationConfiguration not initialized. Error: {e}")
            
        attrib_val = getattr(ApplicationConfiguration, attrib, None)
        if attrib_val is None:
            return default
        return attrib_val

# Example usage:
# config = ApplicationConfiguration("config.ini")
# print(ApplicationConfiguration.get_attr("logging", "level"))