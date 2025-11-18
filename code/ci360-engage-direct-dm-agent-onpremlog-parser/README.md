# Engage_Direct_DM_Agent_Log_Parser

A set of executable/SAS code to parse the DM Agent log (onprem_direct.log) from Engage:Direct.   The parsed output is a set to CSV files that can be used by any reporting too.  SAS code for generating a set of sample reports is included.

One goal of this project is to keep things as simple as possible for the user.  Accordingly, there is nothing to install.  You simply copy the Windows/Linux parser executable (dirlogparse) to a folder/directory of your choice and run.  These instructions are slanted towards the Linux user, but Windows directions are similar.

A sample script is provided, but you simply execute the parser in this manner:
    Navigate to the folder where you copied the executable
    ./dirlogparse -f  /your/directory/path/onprem-direct.log
Two CSV files are generated in the same directory:
    tasks.csv
    subtasks.csv

You can use these CSV files to generate reports using a tool of your choice.  I have included sample SAS code that you could use/adapt to your requirements.  Use SAS Studio or EG to run them.  You only need to update the directory/folder where you generated the CSV files above.

Pre-requisites:
    Ensure that XCMD is enabled for your SAS session. 
        https://go.documentation.sas.com/doc/en/webeditorcdc/3.8/webeditorag/n0zgqoiah057f3n1pjd8uvoh9pbx.htm

If you choose to execute dirlogparse external to SAS (the function performed by 0_ParseLog.sas) you do not need to enable XCMD.
This is not described in this document.

Following are the sample reports.  See reports folder for report snippets.
-   List of tasks executed in chronological order with execution times 
-   Task execution detail for each task with sub-tasks showing a breakup of execution time
-   Stored Processes and their execution times (in seconds)
-   A plot of the number of threads used at different times of day
