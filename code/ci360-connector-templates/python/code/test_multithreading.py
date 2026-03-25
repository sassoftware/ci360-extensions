#Copyright © 2025, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
#SPDX-License-Identifier: Apache-2.0

from common.appCommon import ThreadedAppCommon
from common.appCommonDefaultLoadBalancer import AppCommonDefaultLoadBalancer #Just importing this class will register the default load balancer methods
from utils.logging import SASCI360VeloxPyLogging
import threading
import time
import asyncio
import datetime

# Define the load balancing method to use: "RR" (Round Robin), "LL" (Least Loaded), "RND" (Random), "LLRND" (Least Loaded Random)
LLMETHOD:str="LLRND"  # Change this to test different load balancing methods
ITERWAIT:int=1  # Time in seconds for each thread to wait between iterations
SIMDELAY:int=1  # Time in seconds for each thread to simulate work
RTCNT:int=1  # Retry count for blocking or atomic operations

@staticmethod
def customPrePostProcessor(*args,**kwargs):
    for arg in args:
        i=0
        ThreadedAppCommon.appMessage(f"Unnamed Argument {i} is: " + str(arg))
        i+=1
    for key in kwargs:
        ThreadedAppCommon.appMessage(f"Named Argument {key} is: " + kwargs.get(key))




class WorkerClass:

    global LLMETHOD
    global ITERWAIT
    global SIMDELAY
    global RTCNT
    @staticmethod
    def threadIDGenerator(*args, **kwargs):
        # return int(datetime.datetime.now().timestamp())
        vAppName=kwargs.get("threadedAppName")
        return "Custom_"+vAppName+"_"+str(ThreadedAppCommon.getThreadCount(vAppName=vAppName))

    # worker_load_distribution_method:str="RR", threadedAppName:str="ThreadedApp", iterationWait:int=0,threadIdGenerator:callable=generateDefaultThreadID, preProcessor:callable =None
    @ThreadedAppCommon.threadedWorker("someAdditionalArgumentValue1", worker_load_distribution_method=LLMETHOD, iterationWait=ITERWAIT,threadIdGenerator=threadIDGenerator, threadedAppName="ClassWorkerApp", additionalNamedArgument1="additionalNamedArgumentValue1")
    @classmethod
    async def classWorker(cls,*args, thread_id:str,simulationDelay:int,vAppName:str, **kwargs):
        ThreadedAppCommon.appMessage(f"In classWorker method for App {vAppName} : Thread {thread_id}", "INFO")
        WorkerClass.BlockingOrAtomicOperationExample(cls,*args, vThreadId=thread_id, vAppName=vAppName, simulationDelay=simulationDelay,**kwargs)
        # WorkerClass.NonBlockingOperationExample(cls,*args, vThreadId=thread_id, vAppName=vAppName, simulationDelay=simulationDelay,**kwargs)
        return
    
    @ThreadedAppCommon.BlockingOrAtomicOperation(retryCount=RTCNT, simulationDelay=SIMDELAY)
    @classmethod
    def BlockingOrAtomicOperationExample(cls,*args,simulationDelay:int=0,**kwargs)-> None:
        # Perform blocking operation here
        vThreadID= kwargs.get("vThreadId","")
        vAppName= kwargs.get("vAppName","")
        ThreadedAppCommon.appMessage(f"Performing blocking / atomic operation for App {vAppName} : Thread {vThreadID}", "INFO")
        ThreadedAppCommon.appMessage(f"There are {ThreadedAppCommon.getThreadCount(vAppName)} active threads for {vAppName} app....")
        ThreadedAppCommon.appMessage(f">>>>>>>>>>Worker for App {vAppName} : thread {vThreadID} is starting blocking or atomic work...", "INFO")
        if simulationDelay > 0 :
            ThreadedAppCommon.appMessage(f"App {vAppName} : Thread {vThreadID} Simulating blocking or atomic work for {simulationDelay} second(s)","DEBUG")
            time.sleep(simulationDelay)  # Simulate work by sleeping
        ThreadedAppCommon.appMessage(f"<<<<<<<<<<Worker for App {vAppName} : thread {vThreadID} has completed its blocking or atomic work at {time.strftime('%X')}", "INFO")
        return
    
    @classmethod
    def NonBlockingOperationExample(cls,*args,simulationDelay:int=0,**kwargs)-> None:
        # Perform non-blocking operation here
        vThreadID= kwargs.get("vThreadId","")
        vAppName= kwargs.get("vAppName","")
        ThreadedAppCommon.appMessage(f"Performing non-blocking / non-atomic operation for App {vAppName} : Thread {vThreadID}", "INFO")
        ThreadedAppCommon.appMessage(f"There are {ThreadedAppCommon.getThreadCount(vAppName)} active threads for {vAppName} app....")
        ThreadedAppCommon.appMessage(f">>>>>>>>>>Worker for App {vAppName} : thread {vThreadID} is starting non-blocking or non-atomic work...", "INFO")
        if simulationDelay > 0 :
            ThreadedAppCommon.appMessage(f"App {vAppName} : Thread {vThreadID} Simulating non-blocking or non-atomic work for {simulationDelay} second(s)","DEBUG" )
            time.sleep(simulationDelay)  # Simulate work by sleeping
        ThreadedAppCommon.appMessage(f"<<<<<<<<<<Worker for App {vAppName} : thread {vThreadID} has completed its non-blocking or non-atomic work at {time.strftime('%X')}", "INFO")
        return

@ThreadedAppCommon.threadedWorker("someAdditionalArgumentValue1", worker_load_distribution_method=LLMETHOD, threadedAppName="StaticWorkerApp",iterationWait=ITERWAIT,threadIdGenerator=WorkerClass.threadIDGenerator,  aquireLockForBlockingOps=True, additionalNamedArgument1="additionalNamedArgumentValue1")
async def staticWorker(*args, thread_id:str,simulationDelay:int,vAppName:str, aquireLockForBlockingOps:bool=False, **kwargs):
    ThreadedAppCommon.appMessage(f"In staticWorker method for App {vAppName} : Thread {thread_id}", "INFO") 
    StaticBlockingOrAtomicOperationExample(vAppName=vAppName, vThreadID=thread_id, simulationDelay=simulationDelay)
    # StaticNonBlockingOperationExample(vAppName=vAppName, vThreadID=thread_id, simulationDelay=simulationDelay)
    return

@ThreadedAppCommon.BlockingOrAtomicOperation(retryCount=RTCNT, simulationDelay=SIMDELAY)
@staticmethod
def StaticBlockingOrAtomicOperationExample(*args, simulationDelay=SIMDELAY, **kwargs)-> None:
    # Perform blocking operation here
    vThreadID= kwargs.get("vThreadID","")
    vAppName= kwargs.get("vAppName","")
    ThreadedAppCommon.appMessage(f"Performing static blocking / atomic operation for App {vAppName} : Thread {vThreadID}", "INFO")
    ThreadedAppCommon.appMessage(f"There are {ThreadedAppCommon.getThreadCount(vAppName)} active threads for {vAppName} app....")
    ThreadedAppCommon.appMessage(f">>>>>>>>>>Worker for App {vAppName} : thread {vThreadID} is starting blocking or atomic work...", "INFO")
    if simulationDelay > 0 :
        ThreadedAppCommon.appMessage(f"App {vAppName} : Thread {vThreadID} Simulating blocking or atomic work for {simulationDelay} second(s)","DEBUG")
        time.sleep(simulationDelay)  # Simulate work by sleeping
    ThreadedAppCommon.appMessage(f"<<<<<<<<<<Worker for App {vAppName} : thread {vThreadID} has completed its blocking or atomic work at {time.strftime('%X')}", "INFO")
    return

@staticmethod
def StaticNonBlockingOperationExample(*args, simulationDelay=SIMDELAY, **kwargs)-> None:
    # Perform non-blocking operation here
    vThreadID= kwargs.get("vThreadID","")
    vAppName= kwargs.get("vAppName","")
    ThreadedAppCommon.appMessage(f"Performing static non-blocking / non-atomic operation for App {vAppName} : Thread {vThreadID}", "INFO")
    ThreadedAppCommon.appMessage(f"There are {ThreadedAppCommon.getThreadCount(vAppName)} active threads for {vAppName} app....")
    ThreadedAppCommon.appMessage(f">>>>>>>>>>Worker for App {vAppName} : thread {vThreadID} is starting non-blocking or non-atomic work...", "INFO")
    if simulationDelay > 0 :
        ThreadedAppCommon.appMessage(f"App {vAppName} : Thread {vThreadID} Simulating non-blocking or non-atomic work for {simulationDelay} second(s)","DEBUG")
        time.sleep(simulationDelay)  # Simulate work by sleeping
    ThreadedAppCommon.appMessage(f"<<<<<<<<<<Worker for App {vAppName} : thread {vThreadID} has completed its non-blocking or non-atomic work at {time.strftime('%X')}", "INFO")
    return



async def main():
    static_app_name = "StaticWorkerApp"
    class_app_name = "ClassWorkerApp"
    app_name = static_app_name
    # app_name = class_app_name
    num_threads = 10
    simulationDelayInterval = 1
    threadedWorker = staticWorker
    # threadedWorker = WorkerClass.classWorker
    # Initialize logging
    appLogging = SASCI360VeloxPyLogging()
    appLogging.loggerName = "TestMultiThreadingApp"
    appLogging.logToFile = False
    appLogging.logToConsole = True
    # appLogging.logFileName = "TestMultiThreading_{LOGGERNAME}_PID{PID}_TH{THREADID}_DTTM{TIMESTAMP}_RUN{RUNID}.log"
    appLogging.logLevel = "TRACE"
    appLogging.startLogger()

    # app_name1 = class_app_name
    # num_threads1 = 5
    # simulationDelayInterval1 = 2
    # # threadedWorker = staticWorker
    # threadedWorker1 = WorkerClass.classWorker
    # AppCommonDefaultLoadBalancer.registerDefaultLoadBalancerMethods()
    #AppCommonDefaultLoadBalancer.registerLoadBalancerClass()

    waitTime= simulationDelayInterval -1
    # waitTime= 5
    for i in range(0,num_threads,1):
        ThreadedAppCommon.appMessage(f"Starting thread {ThreadedAppCommon.getThreadCount(vAppName=app_name)+1}", "INFO")
        await ThreadedAppCommon.createAppThread(simulationDelay = simulationDelayInterval, vAppName=app_name, vTarget=threadedWorker)

    
    ThreadedAppCommon.appMessage(f"Waiting {waitTime} seconds for all {app_name} threads to complete...", "INFO")
    if waitTime > 0:
        time.sleep(waitTime)  # Main thread waits before terminating threads

    ThreadedAppCommon.appMessage(f"Now signalling all {app_name} threads to stop working...", "INFO")
    await ThreadedAppCommon.signalStopToAppThreads(app_name)  # Signal all threads to terminate for this application class
    ThreadedAppCommon.appMessage(f"Waiting {waitTime} seconds for all {app_name} threads to stop working...", "INFO")
    if waitTime > 0:
        time.sleep(waitTime)  # Main thread waits before terminating threads
    ThreadedAppCommon.appMessage(f"Now stopping all {app_name} threads...", "INFO")
    await ThreadedAppCommon.closeAllAppThreads(app_name)
    ThreadedAppCommon.appMessage(f"All {app_name} threads have completed.", "INFO")


    return

if __name__ == "__main__":
    asyncio.run(main()) # For Python 3.7+
 