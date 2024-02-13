/******************************************************************************/
/* Copyright © 2023, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved. */
/* SPDX-License-Identifier: Apache-2.0                                        */
/* ****************************************************************************/
%global
        ci360_log_path
        token
        diBatchControlIn
        gdprDeleteTable
        eventLogTable
        IB_METADATALIB
        IB_DBOLIB
        IB_STAGELIB
        DSC_TENANT_ID
        DSC_SECRET_KEY
        CI360_server
        TokenGenMethod
        DSC_CONFIG_PATH
        PYTHON_PATH
    ;

/* ***************************************/
/* CONSTANTS - SHOULD NOT NEED TO CHANGE */
/* ***************************************/
%let ci360_log_path = &IB_GDPR_DELETE_PATH./logs;

/* ***************************************/
/*    ALWAYS CUSTOMIZE THESE VARIABLES   */
/* ***************************************/

/* CI360-specific variables */
%let DSC_TENANT_ID =%str();
%let DSC_SECRET_KEY=%str();
%let CI360_server = extapigwservice-training.ci360.sas.com;

/* Datamart-specific variables */
%let diBatchControlIn = TABLE_ADMIN;
%let gdprDeleteTable = CI360_GDPR_DELETE;
%let eventLogTable = CI360_EVENT_LOG;


/*Libnames*/
%let IB_METADATALIB = CMDM;
%let IB_DBOLIB = CMDM;
%let IB_STAGELIB = CMDM;

/*===================================== GLOBAL ===================================*/
/* Compile all macro files                                                        */
/*================================================================================*/
options 
    append      =  (sasautos="&IB_GDPR_DELETE_PATH./macro")
    append      =  (sasautos="&IB_GDPR_DELETE_PATH./util")
;
options mprint mlogic symbolgen;

%PUT NOTE: &=IB_GDPR_DELETE_PATH;

%check_tables_ddl;

%let TokenGenMethod=python;
%let DSC_CONFIG_PATH = &IB_GDPR_DELETE_PATH./util;
%let PYTHON_PATH = python;
%gentoken;

%let token = &DSC_AUTH_TOKEN.;


