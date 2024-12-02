#*************************************************************************************************************#
# Program Name: serviceConfig.py                                                                              #
# Program Description: This is a configuration file for the utility. Users can define values for parameters   #
#                      that help in executing/hosting the service on the Server.                              #                                                                                                             #
# Author: Global Customer Intelligence Practice                                                               #
# Date: 11-October-2024                                                                                       #
#                                                                                                             #
# Copyright  2023, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.                                   #
# SPDX-License-Identifier: Apache-2.0                                                                         #
#*************************************************************************************************************#
class svcConfig(object):   
    # IP address where service will be hosted 
    host = '127.0.0.10'  
    # Port number where service will be run on
    port = 8100
    # Set to 'True' if you want to enable debugging or else set to 'False'
    debug = 'False'
