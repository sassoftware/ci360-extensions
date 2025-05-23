/*******************************************************************************/
/* Copyright(c) 2025, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved. */
/* SPDX-License-Identifier: Apache-2.0                                         */
/* *****************************************************************************/
%macro dm_agent_connectivity_check(agent_host_name=localhost, agent_port=10080);

	/* Get DM Agent diagnistics */
	filename outfile temp;
	proc http method=get ct="json\application" timeout=15
		/* Note that this API call is not a part of a Public API and CAN BE SUBJECT TO CHANGE. */
		url="http://&agent_host_name.:&agent_port./CIOnPremDirect/rest/commons/diagnostics"
		out=outfile;
	run;

	/* Check response */
	%if %sysfunc(fexist(outfile))=0 %then %do;
		%put ERR%str()OR: Direct Marketing Agent is not responding. Restart agent please.;
	%end;
	%else %do;
		libname outfile json;

		/* Check response for web socket connection to CI360 */
		%let ci360_health=0;
		%if %sysfunc(exist(outfile.CI360_GATEWAY_HEALTH_CHECK)) %then %do;
			PROC SQL noprint;
				select healthy, message into :ci360_health, :ci360_message from outfile.CI360_GATEWAY_HEALTH_CHECK;
			QUIT;
		%end;

		/* Check response for connection to SAS Server */
		%let sas_health=0;
		%if %sysfunc(exist(outfile.SAS_HEALTH_CHECK)) %then %do;
			PROC SQL noprint;
				select healthy, message into :sas_health, :sas_message from outfile.SAS_HEALTH_CHECK;
			QUIT;
		%end;

		/* Log responses */
		%if &ci360_health ne 1 %then %do;
			%put ERR%str()OR: &ci360_message..;
			%put ERR%str()OR-  Check the connection details and restart the Direct Marketing Agent.;
		%end;
		%else %do;
			%put NOTE: &ci360_message..;
		%end;
		%if &sas_health ne 1 %then %do;
			%put ERR%str()OR: &sas_message..;
			%put ERR%str()OR- Check status of the SAS Server, the connection details and restart the Direct Marketing Agent.;
		%end;
		%else %do;
			%put NOTE: &sas_message..;
		%end;
	%end;
%mend dm_agent_connectivity_check;

/* No parameters needed for single machine with default dm agent port */
%dm_agent_connectivity_check; 

/* With Parameters when agent is not on SAS server or other/multiple agents/ports */
/*%dm_agent_connectivity_check(agent_host_name=sas-aap, agent_port=10080);*/
