import logging
import sys
import configparser
import inspect
import time
import builtins
import os
from typing import List
import random
import threading
import re 

DEV:int=100
TRACE:int=5
@staticmethod
def trace(self, message, *args, **kws) -> None:
    if self.isEnabledFor(TRACE):
        self._log(TRACE, message, args, **kws)

@staticmethod
def dev(self, message, *args, **kws) -> None:
    if self.isEnabledFor(DEV):
        self._log(DEV, message, args, **kws)



class SASCI360VeloxPyLogging():
    global trace
    global TRACE
    global dev
    global DEV
    global SEQ
    logConfigList:dict = {"Logging_Configurations"  : 
                            {"logfilelocation":"logFilePath", 
                             "loggername":"loggerName", 
                             "loglevel":"logLevel", 
                             "loglineformat":"logLineFormat", 
                             "logdateformat":"logDateFormat", 
                             "developermode":"developerMode", 
                             "logtofile":"logToFile", 
                             "logtoconsole":"logToConsole",
                             "logfilename":"logFileName"
                             }
                            }

    def __init__(self):
        # Initialize the logging configuration
        self.__logBuffer = []
        self.__logBufferMaxLength = 100
        self.__loggingConfig:configparser.SectionProxy = None
        self.timeStamp:str = ""
        self.__dir_separator:str = ""
        self.eventlogger:logging.Logger = None
        self.eventloggers: List[logging.Logger] = []
        self.consoleLoggerHandler:logging.Handler = None
        self.consoleErrorLoggerHandler:logging.Handler = None
        self.logDateFormat:str = "%m/%d/%Y %I:%M:%S %p"
        self.logLineFormat:str = "%(asctime)s | %(name)s | %(levelname)s | %(message)s"
        self.logLevel:str = "INFO"
        self.eventlogfileHandler:logging.Handler = None
        self.logFilePath = "."
        self.logToConsole:bool = True
        self.logToFile:bool  = True
        self.developerMode:bool = False
        self.loggerName:str = "SASCI360VeloxPyDefualt"
        if sys.platform.find("win") == 0:
            self.__dir_separator="\\"
        else:
            self.__dir_separator="/"
        self.timeStamp:str = None
        self.pid = None
        self.runID = None
        self.logFileName = "Prefix_{LOGGERNAME}_PID{PID}_TH{THREADID}_DTTM{TIMESTAMP}_RUN{RUNID}_Suffix.log"

    class __stdErrFilter(logging.Filter):
        def filter(self, record):
            return record.levelno > logging.WARNING

    class __stdOutFilter(logging.Filter):
        def filter(self, record):
            return record.levelno <= logging.WARNING


    def __flushLogBuffer(self):
        if self.eventlogger == None or len(self.eventlogger.handlers) == 0:
            if not self.logToConsole:
                for logMsg in self.__logBuffer:
                    # write buffrred messages to console
                    print(f"NOLOG: {logMsg[0]} : {logMsg[1]}")
        else:
            if len(self.__logBuffer) > 0:
                for logMsg in self.__logBuffer:
                    # write buffrred messages to log handler(s)
                    self.__writeLogMessage(logMsg[0],logMsg[1])
        self.__logBuffer =[]

    
    def __writeLogMessage(self,vStrLogMessage,vLevel):
        match str.upper(vLevel):
            case "DEV":
                self.eventlogger.dev(self=self.eventlogger,message=vStrLogMessage)
            case "TRACE":
                self.eventlogger.trace(self=self.eventlogger,message=vStrLogMessage)
            case "DEBUG":
                self.eventlogger.debug(vStrLogMessage)
            case "INFO":
                self.eventlogger.info(vStrLogMessage)
            case "WARN":
                self.eventlogger.warning(vStrLogMessage)
            case "ERROR":
                self.eventlogger.error(vStrLogMessage)
            case "CRITICAL":
                self.eventlogger.critical(vStrLogMessage)
            case _:
                self.eventlogger.info(vStrLogMessage)

    
    
    def __initializeLoggingConfig(self,vAppConfigINI: str | configparser.ConfigParser):
        if type(vAppConfigINI) == str:
            vConfigParser = configparser.ConfigParser(interpolation=None)
            try:
                if os.path.exists(vAppConfigINI):
                    vConfigParser.read(vAppConfigINI, encoding='utf-8')
                    self.__loggingConfig = vConfigParser
                else:
                    print(f"Configuration file {vAppConfigINI} does not exist. Using default logging configuration.")
                    self.__loggingConfig = None
            except Exception as e:
                print(f"Error reading config file {vAppConfigINI} : {e}. Using default logging configuration.")
                self.__loggingConfig
        elif type(vAppConfigINI) == configparser.ConfigParser:
            try:
                if "Logging_Configurations" in vAppConfigINI:
                    self.__loggingConfig = vAppConfigINI
                else:
                    print("Error: 'Logging_Configurations' section not found in config file. Using default logging configuration.")
                    self.__loggingConfig = None
            except Exception as e:
                print(f"Error processing config parser: {e}. Using default logging configuration.")
                self.__loggingConfig = None 


    
    def __queueLogMessage(self,vStrLogMessage,vLevel):
        if self.eventlogger == None or len(self.eventlogger.handlers) == 0:
            if self.logToConsole:
                print(f"NOLOG: {vLevel} : {vStrLogMessage}")
        elif self.eventlogger != None and len(self.eventlogger.handlers) > 0:
            if self.logToFile and self.eventlogfileHandler == None:
                if len(self.__logBuffer) < self.__logBufferMaxLength:
                    self.__logBuffer.append([vStrLogMessage,vLevel])
                else:
                    #__flushLogBuffer(logToConsole,eventlogger)
                    self.__logBuffer.append([vStrLogMessage,vLevel])
                    #__flushLogBuffer()
            else:    
                self.__writeLogMessage(vStrLogMessage,vLevel)

    def __generateLogFileName(self) ->str:
        try:
            # self.timeStamp = time.strftime(self.logDateFormat "%Y%m%d%H%M%S")
            # self.timeStamp = time.strftime(re.sub(r'%(?![YmdHMSI])[^%]*|[^%YmdHMSI]+','',self.logDateFormat))
            self.timeStamp = time.strftime(re.sub(r'%(?![YymdbHIpMSaj+])[^%]*|[^%YymdbHIpMSaj+]+','_',self.logDateFormat))
            self.pid = os.getpid().__str__()
            self.runID = random.randint(100, 999).__str__()
            return self.logFileName.format(LOGGERNAME=self.loggerName ,TIMESTAMP=self.timeStamp, PID=self.pid, RUNID=self.runID, THREADID=threading.get_ident())
        except Exception as e:
            print(f"Error getting log file suffix: {e}")
            return eval(f"{self.loggerName}_{self.timeStamp}_{self.pid}_{self.runID}.log")
    
    def initializeLogging(self,vAppConfigINI: str | configparser.ConfigParser = "sasci360veloxpy.ini", forceReInitializeLogger: bool = False):
        self.stopLogger()
        if self.__loggingConfig == None or forceReInitializeLogger:
            self.__initializeLoggingConfig(vAppConfigINI)
            if self.__loggingConfig != None:
                for vConfigSection in SASCI360VeloxPyLogging.logConfigList:
                    for vConfigParameter in SASCI360VeloxPyLogging.logConfigList[vConfigSection]:
                        vValue = object()
                        vAttrName= SASCI360VeloxPyLogging.logConfigList[vConfigSection][vConfigParameter]
                        vAttrVal = getattr(self, vAttrName, None)
                        if vAttrVal != None:
                            try:
                                match type(vAttrVal):
                                    case builtins.bool:
                                        vValue = str.upper(self.__loggingConfig[vConfigSection][vConfigParameter]) == "YES"
                                    case builtins.int:
                                        vValue = int(self.__loggingConfig[vConfigSection][vConfigParameter])
                                    case builtins.str:
                                        vValue = str(self.__loggingConfig[vConfigSection][vConfigParameter])
                                    case builtins.list:
                                        vValue = self.__loggingConfig[vConfigSection][vConfigParameter].split(",")
                                    case builtins.dict:
                                        vValue = self.__loggingConfig[vConfigSection][vConfigParameter]
                                    case builtins.float:
                                        vValue = float(self.__loggingConfig[vConfigSection][vConfigParameter])
                                    case _:
                                        vValue = self.__loggingConfig[vConfigSection][vConfigParameter]
                            except configparser.InterpolationError: 
                                print(f"InterpolationError: {vConfigParameter} in section {vConfigSection} could not be interpolated. Using default value.")
                                continue
                            except KeyError:
                                print(f"KeyError: {vConfigParameter} not found in section {vConfigSection}. Using default value.")
                                continue
                            except configparser.NoOptionError:
                                print(f"NoOptionError: {vConfigParameter} not found in section {vConfigSection}. Using default value.")
                                continue
                            except configparser.NoSectionError:
                                print(f"NoSectionError: Section {vConfigSection} not found in config. Using default value.")
                                break
                            except configparser.Error as e:
                                print(f"ConfigParser Error: {e}. Using default value for {vConfigParameter} in section {vConfigSection}.")
                                continue
                            except Exception as e:
                                print(f"Unexpected error while reading config {vConfigParameter} in section {vConfigSection}: {e}. Using default value.")
                            else:
                                setattr(self,vAttrName,vValue)
                                continue
            
            self.eventlogger = logging.Logger(self.loggerName+"_logger")
            #Define additional custom logging level
            logging.addLevelName(TRACE, "TRACE")
            logging.Logger.trace = trace
            logging.addLevelName(DEV, "DEV")
            logging.Logger.dev = dev
        else:
            self.writeLogMessage("Logging configuration already initialized. Skipping re-initialization.")
            self.__flushLogBuffer()
        return
    
    
    def enableConsoleLogging(self):
        try:
            # Initialize Basic Logging to STDOUT and STDERR
            if self.logToConsole:
                self.consoleLoggerHandler=logging.StreamHandler(sys.stdout)
                self.consoleLoggerHandler.formatter=logging.Formatter(self.logLineFormat,self.logDateFormat)
                self.consoleLoggerHandler.addFilter(self.__stdOutFilter())
                self.eventlogger.addHandler(self.consoleLoggerHandler)
                self.consoleErrorLoggerHandler=logging.StreamHandler(sys.stderr)
                self.consoleErrorLoggerHandler.formatter=logging.Formatter(self.logLineFormat,self.logDateFormat)
                self.consoleErrorLoggerHandler.addFilter(self.__stdErrFilter())
                self.eventlogger.addHandler(self.consoleErrorLoggerHandler)
                if self.logLevel != None and self.logLevel != "":
                    self.consoleLoggerHandler.setLevel(logging.getLevelNamesMapping()[self.logLevel])
                else:
                    self.writeLogMessage("Log Level not defined. Default logging level for console loggers is INFO","INFO")
                    self.consoleLoggerHandler.setLevel(logging.getLevelNamesMapping()["INFO"])
                self.writeLogMessage("Console Logger Initialized","TRACE")
            else:  
                self.writeLogMessage("Console Logging is disabled. No output to console.","INFO")      
            return
        except Exception as e:
            print(f"Error enabling console logging: {e}")
            sys.exit(1)
    

    
    def disableConsoleLogging(self):
        try:
            if self.consoleLoggerHandler != None:
                self.consoleLoggerHandler.close()
                self.eventlogger.removeHandler(self.consoleLoggerHandler)
                self.consoleLoggerHandler = None
            if self.consoleErrorLoggerHandler != None:
                self.consoleErrorLoggerHandler.close()
                self.eventlogger.removeHandler(self.consoleErrorLoggerHandler)
                self.consoleErrorLoggerHandler = None
        except Exception as e:
            print(f"Error disabling console logging: {e}")
            sys.exit(1)

  
    def enableFileLogging(self):
        try:
            if self.logToFile :
                vFileName = self.logFilePath+ self.__dir_separator + self.__generateLogFileName()
                self.eventlogfileHandler=logging.FileHandler(vFileName,"w","UTF-8",False)
                if self.logLevel != None and self.logLevel != "":
                    self.eventlogfileHandler.setLevel(logging.getLevelNamesMapping()[self.logLevel])
                else:
                    self.writeLogMessage("Log Level not defined. Default logging level for file logger is INFO","INFO")
                    self.eventlogfileHandler.setLevel(logging.getLevelNamesMapping()["INFO"])
                self.eventlogfileHandler.formatter = logging.Formatter(self.logLineFormat,self.logDateFormat)
                self.eventlogger.addHandler(self.eventlogfileHandler)
                self.__flushLogBuffer()
                self.writeLogMessage(f"Log file is : {vFileName}.log","TRACE")
            elif not self.logToFile:
                self.writeLogMessage("Logging to file is disabled. No log file will be generated.","INFO")
            
            if self.logToConsole:
                self.writeLogMessage("Logging to console is turned on. Output will be displayed in console.","INFO")
            else:
                self.writeLogMessage("Logging to console is turned off. No output to console.","INFO")

            return
        except Exception as e:
            print(f"Error enabling file logging: {e}")
            sys.exit(1)
    
    
    def disableFileLogging(self):
        try:
            if self.eventlogfileHandler != None:
                self.eventlogfileHandler.close()
                self.eventlogger.removeHandler(self.eventlogfileHandler)
                self.eventlogfileHandler = None
        except Exception as e:
            print(f"Error disabling file logging: {e}")
            sys.exit(1)


    
    def startLogger(self,vAppConfigINI: str | configparser.ConfigParser = "sasci360veloxpy.ini",forceReInitializeLogger: bool = False):
        try:
            self.initializeLogging(vAppConfigINI=vAppConfigINI, forceReInitializeLogger=forceReInitializeLogger)
            if self.logToConsole:
                self.enableConsoleLogging()
            if self.logToFile:
                self.enableFileLogging()
        except Exception as e:
            print(f"Error starting logger: {e}")
            sys.exit(1)

    
    def stopLogger(self):
        if self.eventlogger != None:
            try:
                self.__flushLogBuffer()
                # self.disableConsoleLogging()
                # self.disableFileLogging()
                for handler in self.eventlogger.handlers:
                    handler.close()
                    self.eventlogger.removeHandler(handler)
                self.eventlogger = None
                self.consoleLoggerHandler = None
                self.consoleErrorLoggerHandler = None
                self.eventlogfileHandler = None 
                self.__loggingConfig = None
                return
            except Exception as e:
                print(f"Error stopping logger: {e}")
                sys.exit(1)
        else:
            return
    

    def developerLog(self,vStrLogMessage: str = None):
        try:
            if self.developerMode:
                vStrDevMessage = "DEVELOPER LOGGING :: STARTS"
                vPadding=""
                stack_info = inspect.stack()
                for frame_info in stack_info:
                    vPadding= vPadding+"  "
                    vStrDevMessage =  vStrDevMessage + "\n" + vPadding + (f"Frame: {frame_info.function}, File: {frame_info.filename}, Line: {frame_info.lineno}")
                    
                if vStrLogMessage == None or vStrLogMessage == "":
                    vStrLogMessage = vStrDevMessage
                else:
                    vStrLogMessage = vStrDevMessage + f" :: {vStrLogMessage}"
                self.__queueLogMessage(vStrLogMessage,"DEV")
                vStrLogMessage = "DEVELOPER LOGGING :: ENDS"
                self.__queueLogMessage(vStrLogMessage,"DEV")

        except Exception as e:
            self.__queueLogMessage(f"Error in printing developerLog: {e}\n DEVELOPER LOGGING :: ENDS","DEV")
        finally:
            return
        
    
    def writeLogMessage(self,vStrLogMessage: str, vLevel="INFO"):
        self.developerLog()
        try:
            if vStrLogMessage == None or vStrLogMessage == "":
                vStrLogMessage = "No message provided"
            else:
                    self.__queueLogMessage(vStrLogMessage,vLevel)
        except Exception as e:
            print(f"Error writing log message: {e}")
            sys.exit(1)