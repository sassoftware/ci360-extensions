#Copyright © 2025, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
#SPDX-License-Identifier: Apache-2.0

from functools import partial
import time, datetime
import os
import configparser
import sys
import importlib
import pathlib
import argparse
from typing import Dict, List, Tuple
import threading
import asyncio
import random

from attr import dataclass
from sqlalchemy import func, inspect, true
from utils.logging import SASCI360VeloxPyLogging


# class to hold common methods and properties for applications
# You can extend this class to add more common methods for your applications
class AppCommon:
    appconfig:configparser.ConfigParser = None
    appClass = None
    args:argparse.ArgumentParser = None


    # Method to display help for the application
    @staticmethod
    def displayHelp():
        return

    # Method to read application configuration from INI file
    # Parameters:
    #   vAppName - name of the application (used to form INI file name)
    #   vClass - class to set the configuration values as attributes
    @staticmethod
    def readAppConfig(vAppName,vClass):
        configValues:dict = dict()
        if AppCommon.fileExists(vAppName+".ini"):
            AppCommon.appconfig = configparser.RawConfigParser()
            AppCommon.appconfig.read(vAppName+'.ini')
            #GCIDCommon.appMessage(f"Reading Configuration File: '{vAppName}.ini'","TRACE")
            for secKey,secValue in AppCommon.appconfig.items():
                for key,value in secValue.items():
                    configValues[key]=value
            for item in configValues:
                setattr(vClass, item, configValues[item])
        else:
            AppCommon.appMessage(f"Application Configuration file '{vAppName}.ini' not found. Exiting...","CRITICAL")
            sys.exit(1)
        return

    # Method to initialize the application
    # Parameters:
    #   vAppClass - class of the application to initialize
    @staticmethod 
    def initApp(vAppClass):
        AppCommon.appClass = vAppClass
        AppCommon.readAppConfig(AppCommon.appClass.appName,AppCommon.appClass)
        if sys.platform.find("win") == 0:
            AppCommon.appClass.slashChar="\\"
        else:
            AppCommon.appClass.slashChar="/"
        AppCommon.appClass.runID = time.strftime("%Y%m%d-%H%M%S")
        AppCommon.args.prog = AppCommon.appClass.appName
        AppCommon.args.description =  AppCommon.appClass.appDescription
        CustomCodeModule=importlib.import_module("GCIDCustom","GCIDCustomCode")
        CustomCodeModule.init()
        return


    # Method to check if an element exists in an array (case-insensitive)
    # Parameters:
    #   vStrParam - string parameter to search for
    #   vArray - array to search in
    @staticmethod
    def isElement(vStrParam,vArray):
        for arg in vArray:
            if str.upper(arg) == str.upper(vStrParam):
                return True
        return False

    # Method to get the index of an element in an array (case-insensitive)
    # Parameters:
    #   vStrKey - string key to search for
    #   vArray - array to search in
    @staticmethod
    def indexOf(vStrKey,vArray):
        AppCommon.appMessage(f"Search {vStrKey} in : {vArray}","TRACE")
        i=0
        for i in range(0,len(vArray)):
            if str.upper(vArray[i]) == str.upper(vStrKey):
                return i
        return -1

    # Method to run a code string using exec
    # Parameters:
    #   vCodeString - code string to execute
    @staticmethod
    def runCode(vCodeString):
        AppCommon.appMessage("Running code","DEBUG")
        AppCommon.appMessage(f"Running code :: {vCodeString}","TRACE")
        exec("GCIDCommon."+ vCodeString)
        return

    # Method to check if a directory exists
    # Parameters:
    #   vStrFullPath - full path of the directory to check 
    @staticmethod
    def directoryExists(vStrFullPath):
        if pathlib.Path.exists(vStrFullPath) and not pathlib.Path.is_file(vStrFullPath):
        #if os.path.exists(vStrFullPath) and not os.path.isfile(vStrFullPath):
            return True
        return False

    # Method to check if a file exists
    # Parameters:
    #   vStrFullPath - full path of the file to check
    @staticmethod
    def fileExists(vStrFullPath):
        #if pathlib.Path.exists(vStrFullPath) and pathlib.Path.is_file(vStrFullPath):
        if os.path.exists(vStrFullPath) and os.path.isfile(vStrFullPath):
            return True
        return False

    # Method to delete a file
    # Parameters:
    #   vStrFullPath - full path of the file to delete
    def deleteFile(vStrFullPath):
        if pathlib.Path.exists(vStrFullPath,True):
            os.remove(vStrFullPath)
        return
    
    # Method to open a file for writing
    # Parameters:
    #   vStrFullPath - full path of the file to open
    def openFile(vStrFullPath):
        fileHandle = None
        try:
            fileHandle = open(vStrFullPath,'w')
        except Exception as e:
            AppCommon.appMessage(f"Exception occurred: {e}")
            return None
        else:
            return fileHandle
        
    # Method to read help file
    # Parameters:   
    #   vHelpFile - path to the help file
    @staticmethod
    def readHelpFile(vHelpFile=""):
        return

    # Method to display help for the application
    @staticmethod
    def displayHelp():
        return()
    
    # Lock for thread-safe printing
    print_lock = threading.Lock()
    # Thread-safe application message logging method
    # Thread-safe print method
    @staticmethod
    def safe_print(*args, **kwargs):
        with ThreadedAppCommon.print_lock:
            print(*args, **kwargs)

    @staticmethod
    def setLoggingLevel(vLogLevel:str) -> None:
        logger= SASCI360VeloxPyLogging.getDefaultLogger()
        if logger != None and len(logger.getLoggers()) > 0:
            match vLogLevel:
                case "DEBUG":
                    logger.setLevelDebug()
                case "INFO":
                    logger.setLevelInfo()
                case "ERROR":
                    logger.setLevelError()
                case "CRITICAL":
                    logger.setLevelCritical()
                case "WARN":
                    logger.setLevelWarning()
                case _:
                    logger.setLevelInfo()
                
    # Method to log application messages
    # Parameters: 
    #   vStrLogMessage - message to log
    #   vLevel - log level (e.g., INFO, DEBUG, ERROR, CRITICAL, WARN)
    @staticmethod
    def appMessage(vStrLogMessage:str,vLevel="INFO"):
        # root_logger = logging.getLogger()
        logger= SASCI360VeloxPyLogging.getDefaultLogger()
        if logger==None or len(logger.getLoggers()) == 0:
            #DEBUG, INFO, ERROR
            ThreadedAppCommon.safe_print(f"{vLevel} : {vStrLogMessage}", flush=True,end="\n")
        else:
            match vLevel:
                case "INFO":
                    logger.writeLogMessage(vStrLogMessage,"INFO")
                case "DEBUG":
                    logger.writeLogMessage(vStrLogMessage,"DEBUG")
                case "ERROR":
                    logger.writeLogMessage(vStrLogMessage,"ERROR")
                case "CRITICAL":
                    logger.writeLogMessage(vStrLogMessage,"CRITICAL")
                case "WARN":
                    logger.writeLogMessage(vStrLogMessage,"WARN")
                case "TRACE":
                    logger.writeLogMessage(vStrLogMessage,"TRACE")
                case _:
                    logger.writeLogMessage(vStrLogMessage,"INFO")
        return

# class to hold metadata for threaded applications
# Parameters:
#   threadedAppName:str - name of the threaded application
#   worker_load_distribution_method:str="RR" - load balancing method for the threaded application
#   iterationWait:int=0 - wait time between iterations for the threaded application
#   threadIdGenerator:callable=None - callable to generate thread IDs for the threaded application
#   preProcessor:callable =None - callable method to plugin any preprocess method before starting the threaded worker
#   appThreads:Dict[str, Tuple[List[threading.Thread], threading.Event, threading.Lock, int, dict[str,int]]] - repository to hold all threaded applications and their threads and related info
#       key = threadedAppName
#       value = Tuple[List[threading.Thread], threading.Event, threading.Lock, int, dict[str,int]]
#       Value Tuple Elements:
#           [0] - List[threading.Thread] - List of threads for the application
#           [1] - threading.Event - Event to signal threads to stop/start
#           [2] - threading.Lock - Lock to synchronize access to shared resources
#           [3] - int - Current Thread Index for round-robin load balancing
#           [4] - dict[str,int] - Dictionary to hold execution count for each thread for least-loaded load balancing
#   loadBalancerMethods:Dict[str, callable] - repository to hold all load balancer methods
#       key = load balancer method name
#       value = callable function which returns next thread to run

# Type aliases for better readability
class AppName(str):
    pass
class RecentThreadIndex(int):
    pass
class AppThreadID(str):
    pass
class AppThreadExecutionCount(int):
    pass
class ThreadsExecutionHistory(Dict):
    pass
class AppThread(threading.Thread):
    pass
class AppThreads(List):
    def getThread(self, index:int)-> AppThread:
        return self[index]
    pass
class App(Tuple):
    def getThreads(self)-> AppThreads:
        return self[0]
    def isEmpty(self)->bool:
        return len(self[0]) == 0
    def getThreadCount(self)->int:
        return len(self[0])
    def getThreadsExecutionHistory(self)-> ThreadsExecutionHistory:
        return self[4]
    def getRecentThreadIndex(self)-> RecentThreadIndex:
        return self[3]
    pass
class AppsDictionary(Dict):
    def getApp(self, appName:str)-> App:
        return self.get(appName,None)
    def findAppThreadInAppThreadPool(self, appName:AppName,threadID:AppThread)-> AppThread:
        for thread in ThreadedAppCommon.appThreads[appName][0]:
            if thread == threadID:
                return thread
        return None
    
    def findLeastRunThreadInAppThreadPool(self,appName:AppName)-> AppThread:
        vApp:App= self.get(appName,None)
        if vApp != None:
            leastLoadCount = float('inf')
            leastLoadedThreadID = None
            for vThread, vLoadCount in App(vApp).getThreadsExecutionHistory().items():
                if vLoadCount < leastLoadCount:
                    leastLoadCount = vLoadCount
                    leastLoadedThreadID = vThread
            return self.findAppThreadInAppThreadPool(appName, leastLoadedThreadID)
        return None
    
    def findLeastLoadedThreadsInAppThreadPool(self,appName:AppName )-> List[AppThread]:
        vApp:App= self.get(appName,None)
        leastLoadedThreads:List[AppThread] = []
        if vApp != None:
            sortedThreadsByLoad = sorted(App(vApp).getThreadsExecutionHistory().items(), key=lambda item: item[1])
            for i in range(0, len(sortedThreadsByLoad)):
                threadId = sortedThreadsByLoad[i][0]
                threadExecutionCount= sortedThreadsByLoad[i][1]
                if i == 0:
                    minExecutionCount = threadExecutionCount
                if threadExecutionCount > minExecutionCount:
                    break
                leastLoadedThread = self.findAppThreadInAppThreadPool(appName, threadId)
                if leastLoadedThread != None:
                    leastLoadedThreads.append(leastLoadedThread)
        return leastLoadedThreads

    def getNextAppThreadFromPool(self,appName:AppName)-> AppThread:
        vApp:App= self.get(appName)
        if vApp != None:
            if not App(vApp).isEmpty():
                nextIndex:RecentThreadIndex= (App(vApp).getRecentThreadIndex()+ 1) % len(App(vApp).getThreads())
                return self.findAppThreadInAppThreadPool(appName, App(vApp).getThreads()[nextIndex])
        return None

    def getRandomThreadFromAppThreadPool(self,appName:AppName)-> AppThread:
        vApp:App= self.getApp(appName)
        if vApp != None:
            if not App(vApp).isEmpty():
            # if ThreadedAppCommon.appThreads[vAppName][0] != None and len(ThreadedAppCommon.appThreads[vAppName][0]) > 0:
                return random.choice(App(vApp).getThreads())
            else:
                return None
            # vThreadsList:List[AppThread]= vApp.getThreads()
            # if len(vThreadsList) > 0:
            #     randomIndex = random.randint(0, len(vThreadsList) - 1)
            #     return vThreadsList[randomIndex]
        return None

    pass

class AppEvent(threading.Event):
    pass
class AppLock():
    def __init__(self, name="MyCustomLock"):
        self._lock = threading.Lock()
        self.name = name
        self._acquired_count = 0
    def isLocked(self):
        return self._lock.locked()
    
    def acquire(self, *args, **kwargs):
        # print(f"[{self.name}] Attempting to acquire lock...")
        result = self._lock.acquire(*args, **kwargs)
        # if result:
        #     self._acquired_count += 1
        #     print(f"[{self.name}] Lock acquired. Acquired count: {self._acquired_count}")
        # else:
        #     print(f"[{self.name}] Lock acquisition failed.")
        return result

    def release(self):
        # print(f"[{self.name}] Releasing lock...")
        if self._lock.locked():
            self._lock.release()
            self._acquired_count -= 1
            # print(f"[{self.name}] Lock released. Acquired count: {self._acquired_count}")

    def __enter__(self):
        self.acquire()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.release()
    pass

class RecentThreadIndex(int):
    pass
class AppThreadID(str):
    pass
class AppThreadExecutionCount(int):
    pass

class ThreadedAppMetadata(AppsDictionary):
    pass


# Type alias for the complex nested structure which holds all threaded applications and their threads and related info
# class AppThreads:ThreadedAppMetadataClass.AppsDictionary[
#                                 ThreadedAppMetadataClass.AppNames,
#                                 ThreadedAppMetadataClass.Apps
#                                     [
#                                         ThreadedAppMetadataClass.AppThreads
#                                                 [
#                                                     ThreadedAppMetadataClass.AppThread
#                                                 ], 
#                                         ThreadedAppMetadataClass.AppEvent, 
#                                         ThreadedAppMetadataClass.AppLock, 
#                                         ThreadedAppMetadataClass.RecentAppIndex, 
#                                         ThreadedAppMetadataClass.ThreadsExecutionHistory
#                                                 [
#                                                     ThreadedAppMetadataClass.AppThreadID,
#                                                     ThreadedAppMetadataClass.AppThreadExecutionCount
#                                                 ]
#                                     ]
#                             ]


class AppMetadata:AppsDictionary[
                                AppName,
                                App
                                    [
                                        AppThreads
                                                [
                                                    AppThread
                                                ], 
                                        AppEvent, 
                                        AppLock, 
                                        RecentThreadIndex, 
                                        ThreadsExecutionHistory
                                                [
                                                    AppThreadID,
                                                    AppThreadExecutionCount
                                                ]
                                    ]
                            ]

                                            
    

# Class to hold common methods and properties for threaded applications
# Inherits from AppCommon class to reuse common application methods
class ThreadedAppCommon(AppCommon):
    
    # appMetaData:ThreadedAppMetadataClass = ThreadedAppMetadataClass()
    # Repository to hold all threaded applications and their threads and related info
    # key = threadedAppName
    # value = Tuple[List[threading.Thread], threading.Event, threading.Lock, int, dict[str,int]]
    # Value Tuple Elements:
    #   [0] - List[threading.Thread] - List of threads for the application
    #   [1] - threading.Event - Event to signal threads to stop/start       
    #   [2] - threading.Lock - Lock to synchronize access to shared resources
    #   [3] - int - Current Thread Index for round-robin load balancing
    #   [4] - dict[str,int] - Dictionary to hold execution count for each thread for least-loaded load balancing
    #appThreads:Dict[str, Tuple[List[threading.Thread], threading.Event, threading.Lock, int, dict[str,int]]] = {} #Dict[str, List[List[threading.Thread], threading.Event, threading.Lock]] = {}
    #AppsDictionary[
                                # AppNames,
                                # Apps[
                                #         AppThreads[
                                #                     AppThread
                                #                     ], 
                                #         AppEvent, 
                                #         ThreadedAppMetadataClass.AppLock, 
                                #         RecentAppIndex, 
                                #         ThreadsExecutionHistoryDict[
                                #                                     AppThreadID,
                                #                                     AppThreadExecutionCount
                                #                                     ]
                                #     ]
                                # ] 
    appThreads:AppMetadata= {}


    # Repository to hold all load balancer methods
    # key = load balancer method name
    # value = callable function which returns next thread to run
    loadBalancerMethods:Dict[str, callable] = {}
    blockingThreads:bool = False

    # Default thread ID generator method
    # Parameters:
    #   *args - unnamed arguments
    #   threadedAppName - name of the threaded application
    #   **kwargs - named arguments
    @staticmethod
    def generateDefaultThreadID(*args, threadedAppName:str, **kwargs)->str:
    #  return int(datetime.datetime.now().timestamp())
        vAppName=threadedAppName
        if ThreadedAppCommon.appThreads[vAppName][0] != None and len(ThreadedAppCommon.appThreads[vAppName][0]) > 0:
            return vAppName+"_"+ str(len(ThreadedAppCommon.appThreads[vAppName][0]))
        else:
            return vAppName + "_"+ str(int(datetime.datetime.now().timestamp()))


    #  Method to get the current thread count for a threaded application
    # Parameters:
    #   vAppName - name of the threaded application
    @classmethod
    def getThreadCount(cls,vAppName:str) -> int:
        if vAppName in ThreadedAppCommon.appThreads:
            if ThreadedAppCommon.appThreads[vAppName][0] != None:
                return len(ThreadedAppCommon.appThreads[vAppName][0])
            else:
                return 0
        else:
            return 0

    @classmethod
    def aquireAppLock(cls,vAppName:str,retryCount:int=3,blocking:bool=False)-> bool:
        # Get the lock for the threaded application from AppMetadata
        vLock:AppLock = cls.appThreads[vAppName][2]
        acquired= False
        for _ in range(retryCount+1):
            acquired= vLock.acquire(blocking=blocking)
            if not acquired:
                time.sleep(1)
            else:
                break
        return acquired

    @classmethod
    def releaseAppLock(cls,vAppName:str)-> None:
        # Get the lock for the threaded application from AppMetadata
        vLock:AppLock = cls.appThreads[vAppName][2]
        vLock.release()
        return

    @classmethod
    def BlockingOrAtomicOperation(cls,*args,retryCount:int=5,**kwargs)-> any:
        def __BlockingOrAtomicOperation(func: callable):
            # Wrapper function to run the threaded worker in an infinite loop until event is set 
            def wrapper(*args, **kwargs):
                vThreadID= kwargs.get("vThreadID","")
                vAppName= kwargs.get("vAppName","")
                ThreadedAppCommon.appMessage(f"Entering Blocking Or Atomic Operations  for App {vAppName} thread {vThreadID}", "DEBUG")
                lockAquired=ThreadedAppCommon.aquireAppLock(vAppName=vAppName, retryCount=retryCount, blocking=False)
                results= None
                if not lockAquired:
                    ThreadedAppCommon.appMessage(f"Could not acquire lock for App {vAppName} : Thread {vThreadID}. Retry in next Thread Iteration", "INFO")
                    return results
                else:
                    ThreadedAppCommon.appMessage(f"Lock acquired for App {vAppName} : Thread {vThreadID}. Proceeding with Atomic Operation", "DEBUG")   
                    if type(func) == classmethod:
                        # Call the classmethod blocking operation function
                        results=func.__wrapped__(*args, **kwargs)
                    else:
                        # Call the static blocking operation function
                        results=func(*args,**kwargs)
                    ThreadedAppCommon.releaseAppLock(vAppName=vAppName)
                    ThreadedAppCommon.appMessage(f"Lock released for App {vAppName} : Thread {vThreadID} after Atomic Operation", "DEBUG")
                ThreadedAppCommon.appMessage(f"Exiting Blocking Or Atomic Operations  for App {vAppName} : Thread {vThreadID}", "DEBUG")
                return results
            return wrapper
        return __BlockingOrAtomicOperation
    

    # Decorator to make a method threaded worker and to be called in an infitinite loop with a delay until event is set
    # worker_load_distribution_method:str= "RR" - REQUIRED - Load balancer method to use for selecting next thread to run
    # threadedAppName:str="ThreadedApp" - OPTIONAL -Threaded Application Name
    # iterationWait:int=0 - OPTIONAL - Forced wait time between iterations in seconds
    # threadIdGenerator:callable=generateDefaultThreadID - OPTIONAL - Callable to generate thread IDs for new threads
    # preProcessor:callable =None - OPTIONAL - Callable method to plugin any preprocess method (if any required) before starting the threaded worker
    @classmethod
    def threadedWorker(cls,*args,worker_load_distribution_method:str="RR",threadedAppName:str="ThreadedApp", iterationWait:int=0,threadIdGenerator:callable=generateDefaultThreadID, preProcessor:callable =None,**kwargs):
        
        # Call the preProcessor method if provided
        if preProcessor != None and callable(preProcessor):
            preProcessor(*args,**kwargs)

        # Register the threaded application
        ThreadedAppCommon.registerThreadedApp(threadedAppName)

        def __threadedWorker(func: callable):

            # Wrapper function to run the threaded worker in an infinite loop until event is set 
            def wrapper(*args, **kwargs):

                # Generate Thread ID using threadIdGenerator or default thread Id Generator class method
                if threadIdGenerator != None and callable(threadIdGenerator) :
                    # Call the custom thread ID generator to get the thread ID
                    vThreadID = threadIdGenerator(*args, threadedAppName=threadedAppName,**kwargs)
                else:
                    # Call the default thread ID generator to get the thread ID
                    vThreadID = ThreadedAppCommon.generateDefaultThreadID(threadedAppName=threadedAppName)

                # Get the event for the threaded application
                vEvent:AppEvent = cls.appThreads[threadedAppName][1]
                
                # Get the lock for the threaded application
                vLock:AppLock = cls.appThreads[threadedAppName][2]

                while not vEvent.is_set():

                    # Determine if this thread should run based on load distribution method
                    ThreadedAppCommon.appMessage(f"Thread {vThreadID} is checking if it should run based on load distribution method '{worker_load_distribution_method}'", "DEBUG")

                    if ThreadedAppCommon.loadBalancerMethods is not None and ThreadedAppCommon.loadBalancerMethods.get(worker_load_distribution_method) is not None:

                        # Get the load balancer method function
                        worker_load_distribution_method_func = ThreadedAppCommon.loadBalancerMethods.get(worker_load_distribution_method)
                        
                        if worker_load_distribution_method_func is None:
                            ThreadedAppCommon.appMessage(f"Load Balancer Method '{worker_load_distribution_method}' not found in registered methods. Thread {vThreadID} will run by default.", "WARN")
                        else:
                            ThreadedAppCommon.appMessage(f"Using Load Balancer Method '{worker_load_distribution_method}' for Thread {vThreadID}", "DEBUG")
                            ThreadedAppCommon.appMessage(f"Function: {worker_load_distribution_method_func}", "TRACE")
                            ThreadedAppCommon.appMessage(f"Function Type: {type(worker_load_distribution_method_func)}", "TRACE")
                            ThreadedAppCommon.appMessage(f"Function is Callable: {callable(worker_load_distribution_method_func)}", "TRACE")
                            ThreadedAppCommon.appMessage(f"Function is Classmethod: {type(worker_load_distribution_method_func) == classmethod}", "TRACE")
                            ThreadedAppCommon.appMessage(f"Function is Staticmethod: {type(worker_load_distribution_method_func) == staticmethod}", "TRACE")
                            ThreadedAppCommon.appMessage(f"Thread Pool Metadata: {ThreadedAppCommon.appThreads}", "TRACE")


                        # Call the load balancer method function to get the next thread to run
                        if type(worker_load_distribution_method_func) == classmethod:
                            # Call the classmethod load balancer method function
                            vNextThread = worker_load_distribution_method_func.__wrapped__(*args, threadedAppName=threadedAppName, threadedAppMetadataDict=ThreadedAppCommon.appThreads,**kwargs)
                        else:
                            # Call the static load balancer method function
                            vNextThread = worker_load_distribution_method_func(*args, threadedAppName=threadedAppName, threadedAppMetadataDict=ThreadedAppCommon.appThreads, **kwargs)
                        if vNextThread != threading.current_thread():
                        
                            # This thread is not selected to run in this iteration
                            ThreadedAppCommon.appMessage(f"Thread {vThreadID} is not selected to run in this iteration. Retrying after {iterationWait} seconds.", "DEBUG")
                            if iterationWait > 0:
                                # wait for the specified iteration wait time before checking again
                                time.sleep(iterationWait)
                            if vEvent.is_set():
                                # Meanwhlile, if the event is set i.e. application threads have been signaled to stop, exit the loop and the thread worker method
                                ThreadedAppCommon.appMessage(f"Thread {vThreadID} detected event is set. Exiting thread worker at {time.strftime('%X')}", "DEBUG")
                                break
                            else:
                                # else continue to next iteration
                                continue
                        else:

                            #  This thread is selected to run in this iteration
                            ThreadedAppCommon.appMessage(f"Thread {vThreadID} is selected to run in this iteration.", "DEBUG")
                    else:

                        # No load balancer method found, run by default
                        ThreadedAppCommon.appMessage(f"No load balancer methods for '{worker_load_distribution_method}' registered. Thread {vThreadID} will run by default.", "WARN")

                    vCurrentThread = threading.current_thread()

                    try:

                        ThreadedAppCommon.appMessage(f"Calling worker method for App {threadedAppName} : thread {vThreadID} at {time.strftime('%X')}", "DEBUG")

                        # call the worker function
                        if type(func) == classmethod:
                            result = asyncio.run(func.__wrapped__(cls,*args, thread_id=vThreadID, vAppName=threadedAppName, **kwargs))
                        else:
                            result = asyncio.run(func(*args, thread_id=vThreadID, vAppName=threadedAppName, **kwargs))

                        # Update the current thread index in the appThreads last element
                        ThreadedAppCommon.appThreads[threadedAppName][3]= ThreadedAppCommon.appThreads[threadedAppName][0].index(vCurrentThread)

                        # increment the execution count for the current thread
                        ThreadedAppCommon.appThreads[threadedAppName][4][vCurrentThread] = ThreadedAppCommon.appThreads[threadedAppName][4].get(vCurrentThread, 0) + 1
                        ThreadedAppCommon.appMessage(f"Worker method for app {threadedAppName} : thread {vThreadID} completed at {time.strftime('%X')}", "DEBUG")
                        if iterationWait > 0:
                            time.sleep(iterationWait)

                    finally:

                        # Release the lock after running the worker function
                        if vLock.isLocked():
                            vLock.release()
                            ThreadedAppCommon.appMessage(f"Thread {vThreadID} released lock after worker finished at {time.strftime('%X')}", "DEBUG")

            return wrapper
        return __threadedWorker

    
    
    # Method to register a threaded application
    # Parameters:
    #   vAppName - name of the threaded application to register
    @classmethod
    def registerThreadedApp(cls,vAppName:str):
        if vAppName not in cls.appThreads:
            initialElement ={f"{vAppName}": [[],AppEvent(),AppLock(),-1,{}]}    
            cls.appThreads.update(initialElement)
        return

    
    
    # Method to create and start a new thread for a threaded application
    # Parameters:
    #   *vArgs - unnamed arguments
    #   vAppName - name of the threaded application
    #   vTarget - target worker function for the thread defined using @ThreadedAppCommon.threadedWorker decorator
    #   **vKwargs - named arguments
    @classmethod
    async def createAppThread(cls,*vArgs,vAppName:str,vTarget:callable,**vKwargs):
        if vAppName not in cls.appThreads:
            raise Exception(f"Worker {vAppName} not registered as 'threadedWorker'")
            sys.exit
        vThread = threading.Thread(target=vTarget, args=vArgs,kwargs=vKwargs)
        ThreadedAppCommon.appMessage(f"Creating thread for app {vAppName}", "INFO")

         # Add the thread to the appThreads dictionary
        ThreadedAppCommon.appMessage(f"Adding thread for app {vAppName}", "DEBUG")
        if vAppName in cls.appThreads:
            cls.appThreads[vAppName][0].append(vThread)
            cls.appThreads[vAppName][4][vThread] = 0
            # cls.appThreads[vAppName][1]=vEvent
        else:
            vEvent = AppEvent()
            vLock=AppLock()
            appElement = { f"{vAppName}" : [[vThread],vEvent,vLock,-1,{vThread:0}]}
            cls.appThreads.update(appElement)
        vThread.start()
        time.sleep(1)  # Give the thread a moment to start
        return vThread

    
    
    # Method to close all threads for a threaded application
    # Parameters:
    #   vAppName - name of the threaded application to close all threads for
    @classmethod
    async def closeAllAppThreads(cls,vAppName:str):
        ThreadedAppCommon.appMessage(f"Closing threads for app {vAppName}", "INFO")
        vThreads:List
        # if AppsDictionary(cls.appThreads).getApp(vAppName) is not None:
        if vAppName in cls.appThreads:
            # vThreads:AppThreads = AppsDictionary(cls.appThreads).getApp(vAppName)
            vThreads = cls.appThreads[vAppName]
            vThreadsList:List = vThreads[0]
            vThreadFromList: threading.Thread
            vEvent:AppEvent = vThreads[1]
            vEvent.set()  # Signal all current threads (if any) to stop
            ThreadedAppCommon.appMessage(f"Signaled threads to stop for app {vAppName}: {vEvent.is_set()}", "INFO")
            ThreadedAppCommon.appMessage(f"Waiting for threads to finish for app {vAppName}", "INFO")
            vLock:AppLock= vThreads[2]
            vLock.acquire()  # Wait to acquire the lock so that the current active threads complete processing and do not start further processing
            # Join all threads
            for vThreadFromList in vThreadsList:
                vThreadFromList.join()
                # vThreadFromList.is_alive
                vThreadsList.remove(vThreadFromList)
            # Wait for all threads to finish
            for vThreadFromList in vThreadsList:
                while vThreadFromList.is_alive():
                    ThreadedAppCommon.appMessage(f"Still waiting for thread {vThreadFromList.name} to finish for app {vAppName}", "DEBUG")
                    time.sleep(1)
            ThreadedAppCommon.appMessage(f"All threads finished for app {vAppName}", "DEBUG")
            vEvent.clear()  # Signal future any new threads to start immediately when created
            vLock.release()  # Release the lock so that future threads can acquire it
             # Clean up the appThreads entry if no threads remain
            # if len(cls.appThreads[vAppName][0]) == 0:
            #     del cls.appThreads[vAppName]
        return
    
    
    
    # Method to signal all threads for a threaded application to stop working
    # Parameters:
    #   vAppName - name of the threaded application to signal stop to all threads for
    @classmethod
    async def signalStopToAppThreads(cls,vAppName:str):
        ThreadedAppCommon.appMessage(f"Stopping threads for app {vAppName}", "INFO")
        vThreads:List
        if vAppName in cls.appThreads:
            vThreads = cls.appThreads[vAppName]
            vEvent:AppEvent = vThreads[1]
            vEvent.set()  # Signal threads to stop
        return
    
    
    
    # Method to register load balancer methods in the loadBalancerMethods repository
    # Parameters:
    #   methodName - name of the load balancer method to register
    #   func - callable function implementing the load balancer method defined using @ThreadedAppCommon.loadBalancerMethodDecorator decorator
    @classmethod
    def registerDefaultLoadBalancerMethod(cls,methodName:str,func:callable):
        if cls.loadBalancerMethods is None:
            cls.loadBalancerMethods = {}
        if methodName not in cls.loadBalancerMethods:
            if func!=None and (callable(func) or type(func) == classmethod):
                cls.loadBalancerMethods[methodName] = func
            else:
                raise Exception(f"Load Balancer Method function is not callable")
        else:
            raise Exception(f"Load Balancer Method '{methodName}' is already registered")
        return
    
    
    
    # Decorator to register load balancer methods
    # You can create a class method or static method and decorate it with this decorator to register it as a load balancer method
    # Parameters:
    #   *args - unnamed arguments   
    #   method:str="DEFAULT" - name of the load balancer method to register
    #   **kwargs - named arguments
    @classmethod
    def loadBalancerMethodDecorator(cls,*args,method:str="DEFAULT", **kwargs):
        def __loadBalancerMethodDecorator(func: callable):
            ThreadedAppCommon.registerDefaultLoadBalancerMethod(method,func)
            def wrapper(*args, **kwargs):
                pass
                # ThreadedAppCommon.appMessage(f"Invoking Load Balancer Method '{method}'", "DEBUG")
                # # Call the load balancer method function
                # if func!=None and callable(func):
                #     ThreadedAppCommon.appMessage(f"Calling Load Balancer Method '{method}'", "TRACE")
                #     ThreadedAppCommon.appMessage(f"Args: {args}", "TRACE")
                #     ThreadedAppCommon.appMessage(f"Kwargs: {kwargs}", "TRACE")
                #     ThreadedAppCommon.appMessage(f"Function: {func}", "TRACE")
                #     ThreadedAppCommon.appMessage(f"Function Type: {type(func)}", "TRACE")
                #     ThreadedAppCommon.appMessage(f"Function is Callable: {callable(func)}", "TRACE")
                #     ThreadedAppCommon.appMessage(f"Function is Classmethod: {type(func) == classmethod}", "TRACE")
                #     ThreadedAppCommon.appMessage(f"Function is Staticmethod: {type(func) == staticmethod}", "TRACE")
                #     ThreadedAppCommon.appMessage(f"Thread Pool Metadata: {ThreadedAppCommon.appThreads}", "TRACE")

                #     # Call the load balancer method function
                #     if type(func) == classmethod:
                #         result = asyncio.run(func.__wrapped__(*args, **kwargs))
                #     else:
                #         result = asyncio.run(func(*args, **kwargs))
                #     return result
                # else:
                #     return None
            return wrapper
        return __loadBalancerMethodDecorator




        
