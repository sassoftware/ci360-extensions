/*-----------------------------------------------------------------------------
 Copyright © 2024, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
 SPDX-License-Identifier: Apache-2.0
-----------------------------------------------------------------------------*/

Data user_profiles;
	profile='training tenant 01';
	External_gateway='https://extapigwservice-training.ci360.sas.com/';
	TENANT_ID='tttttttttttttttttttttttt';	
	CLIENT_SECRET='ssssssssssssssssssssssssssssssssssssssssssssssss';
	API_USER='API-uuuuuuu-uuuu';
	API_PASSWORD='ppppppppppppppppppppppp';
	output;

	profile='training tenant 02';
	External_gateway='https://extapigwservice-training.ci360.sas.com/';
	TENANT_ID='tttttttttttttttttttttttt';	
	CLIENT_SECRET='ssssssssssssssssssssssssssssssssssssssssssssssss';
	API_USER='API-uuuuuuu-uuuu';
	API_PASSWORD='ppppppppppppppppppppppp';
	output;

	profile='Demo tenant 01';
	External_gateway='https://extapigwservice-demo.ci360.sas.com/';
	TENANT_ID='tttttttttttttttttttttttt';	
	CLIENT_SECRET='ssssssssssssssssssssssssssssssssssssssssssssssss';
	API_USER='API-uuuuuuu-uuuu';
	API_PASSWORD='ppppppppppppppppppppppp';
	output;

	profile='Customer EU 01';
	External_gateway='https://extapigwservice-prod-eu.ci360.sas.com/';
	TENANT_ID='tttttttttttttttttttttttt';	
	CLIENT_SECRET='ssssssssssssssssssssssssssssssssssssssssssssssss';
	API_USER='API-uuuuuuu-uuuu';
	API_PASSWORD='ppppppppppppppppppppppp';
	output;

	profile='Customer US 01';
	External_gateway='https://extapigwservice-prod.ci360.sas.com/';
	TENANT_ID='tttttttttttttttttttttttt';	
	CLIENT_SECRET='ssssssssssssssssssssssssssssssssssssssssssssssss';
	API_USER='API-uuuuuuu-uuuu';
	API_PASSWORD='ppppppppppppppppppppppp';
	output;
run;