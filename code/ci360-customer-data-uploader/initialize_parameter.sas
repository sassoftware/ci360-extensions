/******************************************************************************/
/* Copyright © 2023, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved. */
/* SPDX-License-Identifier: Apache-2.0                                        */
/* ****************************************************************************/
%global
        ci360_log_path
        DSC_TENANT_ID
        DSC_SECRET_KEY
        CI360_server
        descriptor_cust
        descriptor_identity
        eventLogTable
        semaphoresTable
        idmapPendingTable
        dataHubTable
        hdsCustConsTable
        custTable
        IB_DBOLIB
        IB_BATCHLIB
        TokenGenMethod
        DSC_CONFIG_PATH
        PYTHON_PATH
        token
    ;

/* ***************************************/
/* CONSTANTS - SHOULD NOT NEED TO CHANGE */
/* ***************************************/

%let ci360_log_path = &IB_UPLOAD_CUST_PATH./logs;

/* ***************************************/
/*    ALWAYS CUSTOMIZE THESE VARIABLES   */
/* ***************************************/

/* CI360-specific variables */

%let DSC_TENANT_ID=%str();
%let DSC_SECRET_KEY=%str();
%let CI360_server=extapigwservice-training.ci360.sas.com;
%let descriptor_cust = CUSTOMER;
%let descriptor_identity = WEB_IDENTITY;

/* Datamart-specific variables */
%let eventLogTable = CI360_EVENT_LOG; 
%let semaphoresTable = TABLE_ADMIN;
%let idmapPendingTable = CI360_IDMAP_PENDING;
%let dataHubTable = DATAHUB;
%let custTable = INDIVIDUAL;

/*Libnames*/

%let IB_DBOLIB = CMDM;
%let IB_BATCHLIB = CMDM;

/*===================================== GLOBAL ===================================*/
/* Compile all macro files                                                        */
/*================================================================================*/
options 
    append      =  (sasautos="&IB_UPLOAD_CUST_PATH./macro")
    append      =  (sasautos="&IB_UPLOAD_CUST_PATH./util")
;
options mprint mlogic symbolgen;

%PUT NOTE: &=IB_UPLOAD_CUST_PATH;

%check_tables_ddl;

%let TokenGenMethod=python;
%let DSC_CONFIG_PATH = &IB_UPLOAD_CUST_PATH./util;
%let PYTHON_PATH = python;
%gentoken;

%let token = &DSC_AUTH_TOKEN.;


