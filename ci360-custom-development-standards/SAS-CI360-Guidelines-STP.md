
# Coding Standards for SAS Stored Process in CI 360



## Overview
Stored Processes (STPs) can be used in **Segment Maps** and on the **Post Process** page of **Direct Marketing Tasks**.

Stored Processes can be created in **SAS Enterprise Guide** or **SAS Management Console**. They consist of:
- SAS code
- Metadata stored on the SAS 9 server

The metadata (name, prompts, keywords) determines how the STP appears in the CI 360 interface.

Metadata must be uploaded from SAS 9 to CI 360 using:

```bash
<DIRECT_AGENT_LOCATION>/dm.sh sstp
```

> **Note**: Changes to STP metadata require re-upload to CI 360. Changes to STP code do **not**.

---

## Stored Process Metadata

### Keywords
Keywords are used by CI 360 to:
- Identify which STPs apply to CI 360
- Determine which macro classes to load
- Describe STP input/output characteristics

Example:
```
CI360
class=all
InSubjectID=Customer
OutSubjectID=Customer
Maxcells=0
```

---

### Stream and Package Result Capabilities

When defining the stored process in metadata you must always check the **Package** box on the Execution tab so that the status and count value are available to the Process Node in CI 360

The **Stream** check box is used to provide extra information to the stored process code via a combination of metadata and code settings:
- add `%maspinit(xmlstream=MacroVar Neighbor);` to the STP code
- add a XML Data Source to the data tab of the stored process metadata. The label and fileref need to correspond to be `macrovar` or `neighbor` and content type `text/xml`.
   - `macrovar` provides information about the task or segment map 
   - `neighbor` provides information about the input and output nodes of the process node

---

## Coding Best Practices

### Code Location

SAS code can be:
- Embedded directly in the Stored Process or 
- Stored in external `.sas` files

**Recommendation**:
- Keep a lightweight wrapper embedded in the Stored Process
- Store business logic in version-controlled external files

---

### Example Stored Process Wrapper Code

```sas
%stpbegin;

options notes mprint;
options nomlogic nosymbolgen nosource2 nofullstimer sastrace=',,,'; /* reduce logging to a nice readable level */

/* required bit – search 'binary_file_copy_cust' in online help for more */
%include "/sso/sfw/CI360Direct/sascode/sasmacro/binary_file_copy_cust.sas";
filename outdata temp lrecl=32767;
%binary_file_copy_cust(infile=livedata, outfile=outdata);
%include outdata;
filename outdata clear;

%maspinit(xmlstream=MacroVar); /* Initiate macrovar table */

%let utilloc=C:\SAS\CI 360 STPs; /* External code and log location */

options nonotes; /* temporarily hide notes */
data _null_; /* dttm adds a timestamp to the log files; */
  dttm=cats(put(today(),yymmddn8.),'_',compress(translate(put(time(),time8.0),'0', ''), ': '));
  call symputx('dttm',dttm);
run;

/* Write log to external file */
proc printto log="&utilloc.\logs\assign_offers_&dttm._&SYSJOBID..log";
run;
options notes;  /* show notes */

%include "&utilloc.\macros\assign_offer_launcher.sas"; /* launches the external code */

proc printto; /* reset log location */
run;

%MACount(&outTable); /* send table row count to CI 360 node */
%MAStatus(&_stpwork.status.txt); /* return status to CI 360 node */

%stpend; 
```

---

## Logging Guidelines

- Use `proc printto` to write logs to accessible locations
- Use dynamic log file names as in the example wrapper code above
- Write custom error messages to the log, but use `%str()` to avoid using the literal word `ERROR` in code. This way the word error only appears in the log when an error occurs.
```sas
%PUT ERR%str()OR: Duplicates in table &in_table. detected during &SYSMACRONAME.; 
```

- Once live, reduce log noise using:

```sas
options notes mprint;
options nomlogic nosymbolgen nosource2 nofullstimer sastrace=',,,';
```
---

## Macro Variables

CI 360 provides runtime information via macro variables.

Documentation:
- Segment Maps: https://go.documentation.sas.com/doc/en/cintcdc/production.a/cintug/seg-process-stp.htm
- Direct Marketing Tasks: https://go.documentation.sas.com/doc/en/cintcdc/production.a/cintug/dir-mrkting-process-node.htm

---

## Macrovar Table

The `macrovar` table contains additional runtime metadata.

Example of extracting export dataset information:

```sas
data _null_; 
  set macrovar;
  length ExportID_sasdata $50;
  retain ExportID_sasdata ;
  if CATEGORY="EXPORTINFO" then do;
	if NAME="EXPORTOUTPUTTYPE" and VALUE="sas" then do;                                         
	  ExportID_sasdata=parent; /* parent value groups rows SAS export */
	end;
	if parent=ExportID_sasdata then do;                                            
		if NAME="EXPORTOUTPUTPATH" then call symputx("export_lib",VALUE,'G');
		if NAME="EXPORTOUTPUTNAME" then call symputx("export_table",VALUE,'G');
	end;
  end;
run;
/* %put &=export_lib &=export_table; */
%mausrexp(&export_lib., Execute); /* Assign export library */
```

---
## Assign libraries in stored process code

- Don't store anything in the MATABLES library. Create your own destination.
- Use `%mausrexp([libname], Execute);` to assign CI 360 export libraries defined in mausrexp.sas.

---

## Code Structure

Recommended folder structure:

```
STP-root/
├─ config/
├─ data/
├─ logs/
└─ macros/
```

The stored process root directory should be accessible to developers and the logs subdirectory should also allow key users access. 
An example for STP-root/ is `/sas/storedprocesses/`.

The config folder contains:
- The `config.sas` sets macro variables for the tenant credentials and database connectivity, as needed
- All environment-specific values are kept in the `config.sas`, not in the macros
- Store metadata files here. Use text files (not datasets) for better version control in Git.

Any physical file output – including logs – should be added to any system maintenance scripts for deletion after a retention period of, e.g., 30 days.

Example project - not a STP though:
https://github.com/sassoftware/ci360-download-client-sas/

---
### Macro Design Guidelines

- Keep macros under ~300 lines of code
- Use parameters for inputs and outputs. Input and output parameters can also be data set names
- Avoid copy/paste forks, refactor shared logic into reusable macros

---
## Testing

- Test your stored process code in SAS Studio or SAS Enterprise Guide. 
- Prepare your SAS session by defining the CI 360 macro variables you will use
- Create the work.macrovar table as needed
- Create input tables as needed
- Exclude CI 360-specific macros. Keep them in the wrapper code mentioned above.
- If `%mausrexp([libname], Execute);` assigns libraries in the wrapper code, assign them manually 
- Save all of the above as a pre-code script.

---
## Generating Code Using Macros

Macros can dynamically generate executable SAS code.

Example pattern:

```sas
filename code temp;

data _null_;
  set sashelp.class;
  file code;
  put '%mean_height_by_age(age=' age ',loop=' _n_ ');';
run;

%include code / source2;
```

---

## Additional Resources

- SAS Programming Tips: https://github.com/sbxjld2/SASTips


