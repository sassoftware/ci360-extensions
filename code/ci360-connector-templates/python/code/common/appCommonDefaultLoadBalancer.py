#Copyright © 2025, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
#SPDX-License-Identifier: Apache-2.0

from common.appCommon import ThreadedAppCommon, AppsDictionary, AppMetadata, App, AppThread
# Class to hold default load balancer methods
# Parameters for each method:
#   *args - unnamed arguments
#   threadedAppName:str - name of the threaded application
#   **kwargs - named arguments
# Each method is defined using @ThreadedAppCommon.loadBalancerMethodDecorator decorator
# Each method returns the next threading.Thread to run based on the load balancing strategy
class AppCommonDefaultLoadBalancer:
    # Method to get the next thread using Round Robin load balancing
    # Parameters:   
    #   *args - unnamed arguments
    #   threadedAppName:str - name of the threaded application
    #   **kwargs - named arguments
    @ThreadedAppCommon.loadBalancerMethodDecorator(method="RR")
    @classmethod
    def getNextThread_round_robin(*args,threadedAppName:str, threadedAppMetadataDict:AppMetadata, **kwargs)-> AppThread:
        vAppName=threadedAppName
        vAppMetadata:App=App(AppsDictionary(threadedAppMetadataDict).getApp(vAppName))
        if not vAppMetadata.isEmpty():
            return AppsDictionary(threadedAppMetadataDict).getNextAppThreadFromPool(vAppName)
        return 

    
    
    
    # Method to get the next thread using Least Loaded load balancing
    # Parameters:
    #   *args - unnamed arguments
    #   threadedAppName:str - name of the threaded application
    #   threadedAppMetadataDict:AppMetadata - metadata dictionary for threaded applications
    #   **kwargs - named arguments
    @ThreadedAppCommon.loadBalancerMethodDecorator(method="LL")
    @classmethod
    def getNextThread_least_loaded(*args, threadedAppName:str, threadedAppMetadataDict:AppMetadata, **kwargs)-> AppThread:
        vAppName=threadedAppName
        vAppMetadata:App=App(AppsDictionary(threadedAppMetadataDict).getApp(vAppName))
        if not vAppMetadata.isEmpty():
            return AppsDictionary(threadedAppMetadataDict).findLeastRunThreadInAppThreadPool(vAppName)
        else:
            return None

    
    
    # Method to get the next thread using Random load balancing
    # Parameters:
    #   *args - unnamed arguments
    #   threadedAppName:str - name of the threaded application
    #   threadedAppMetadataDict:AppMetadata - metadata dictionary for threaded applications
    #   **kwargs - named arguments
    @ThreadedAppCommon.loadBalancerMethodDecorator(method="RND")
    @classmethod
    def getNextThread_random(*args, threadedAppName:str, threadedAppMetadataDict:AppMetadata,**kwargs)-> AppThread:
        vAppName=threadedAppName
        return AppsDictionary(threadedAppMetadataDict).getRandomThreadFromAppThreadPool(vAppName)
    

    # Method to get the next thread using Custom load balancing
    # Parameters:
    #   *args - unnamed arguments
    #   threadedAppName:str - name of the threaded application  
    #   threadedAppMetadataDict:AppMetadata - metadata dictionary for threaded applications
    #   **kwargs - named arguments
    @ThreadedAppCommon.loadBalancerMethodDecorator(method="LLRND")
    @classmethod
    def getNextThread_least_loaded_random(*args, threadedAppName:str, threadedAppMetadataDict:AppMetadata, **kwargs)-> AppThread:
        vAppName=threadedAppName
        vAppMetadata:App=App(AppsDictionary(threadedAppMetadataDict).getApp(vAppName))
        if not vAppMetadata.isEmpty():
            leastLoadedThreadsList=AppsDictionary(threadedAppMetadataDict).findLeastLoadedThreadsInAppThreadPool(vAppName)
            if len(leastLoadedThreadsList) > 0:
                import random
                return random.choice(leastLoadedThreadsList)
        else:
            return None