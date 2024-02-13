/******************************************************************************/
/* Copyright © 2023, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved. */
/* SPDX-License-Identifier: Apache-2.0                                        */
/* ****************************************************************************/
%macro gentoken();
    %if &TokenGenMethod=python %then
    %do;
        /******************************************************************************
        Generate the Authentication token using python function
        verify if the python token generation works correctly from OS command line e.g.
        c:\python36\python.exe c:\generatejwt.py --tenantId XXXXX --secretKey XXXXXXXX
        ******************************************************************************/
        %* python command to generate the authentication token ;

        data python_cmd ;
            dsc_config_path=symget('DSC_CONFIG_PATH');
            python_path=symget('PYTHON_PATH');

            python_function_file=strip(DSC_CONFIG_PATH)||'/generatejwt.py';
            tenantId=symget('DSC_TENANT_ID');
            secretKey=symget('DSC_SECRET_KEY');
            pythoncmd=strip(python_path) || ' ' || strip(python_function_file) || ' --tenantId ' || strip(tenantId) || ' --secretKey ' || strip(secretKey);
            call symput ('pythoncmd',trim(pythoncmd));
            put pythoncmd;
        run;
        %* assign filename to route the output of python command in file ;
        filename oscmd pipe "&pythoncmd.";

        %* read the token value returned by python command ;
        data jwtToken;
            infile oscmd;
            input;
            TokenVal= _infile_;
        run;
        %* de-assign python command file ;
        filename oscmd ;
        %*;
        data _null_;
            set jwtToken;
            call symputx('DSC_AUTH_TOKEN',TokenVal);
        run;
    %end;
    %else %do;
        data _null_;
            header='{"alg":"HS256","typ":"JWT"}';
            payload='{"clientID":"' || strip(symget("DSC_TENANT_ID")) || '"}';
            encHeader=translate(put(strip(header),$base64x64.), "-_ ", "+/=");
            encPayload=translate(put(strip(payload),$base64x64.), "-_ ", "+/=");
            key=put(strip(symget("DSC_SECRET_KEY")),$base64x100.);
            digest=sha256hmachex(strip(key),catx(".",encHeader,encPayload), 0);
            encDigest=translate(put(input(digest,$hex64.),$base64x100.), "-_ ", "+/=");
            token=catx(".", encHeader,encPayload,encDigest);
            call symputx("DSC_AUTH_TOKEN",token);
        run;
    %end;
    %PUT NOTE: !-------------------------------------!;
    %PUT NOTE: &=DSC_AUTH_TOKEN;
    %PUT NOTE: !-------------------------------------!;
%mend gentoken;