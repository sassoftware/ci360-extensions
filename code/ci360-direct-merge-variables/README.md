# CI360_Direct_Merge_Variables

## Name
CI360 Direct Merge Variables

## Description
This stored process is intended to be used as a post process in the CI360 Engage Direct DM Task.   It updates any SAS datasets exported in that task, with corresponding dataitems from other SAS datasets.  Please review the included WORD document in the project for a description of the use cases where the use of this post process is most appropriate.

## Installation

1.	Import the STP package CI360_Direct_Merge_Variables.spk
2.	Copy the sas code file CI360_Direct_Merge_Variables.sas to a directory of your choice.   Optionally, update the following two lines in the code
%let logdir=;
%leg bkpdir=;
For logdir refer to the logging section of this document below.
The variable bkpdir can be used to specify a location to backup the export dataset(s) from ci360 that are input to this process.  You may specify any directory/folder of your choice except the directory that you are using in ci360 to write the export SAS datasets.  There is no need to change this variable, except when you are debugging this process.
3.	Use SMC/EG to modify the stored process that was imported in step #1
a.	Update the line that begins with %include.  The file path must match the location that you specified in step #2
4.	Run the appropriate dm commands to make the new stored process available in ci360.
a)	sstp	- send stored processes
b)	cim	- clear ci360 cache


## Usage
This step is intended to be used as a preprocess step prior to generating a fulfillment vendor ready export (typically CSV) file.  So, a DM task that uses this STP as a post process may include two post process nodes chained in sequence.

## Support
Raja Marla (raja.marla@sas.com)

## Roadmap
If you have ideas for releases in the future, it is a good idea to list them in the README.

## Contributing
If you are aware of an enhancement that will make this process more useful, please share.

## Project status
Active
