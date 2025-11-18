Program:    dirlogparse
    
Usage of dirlogparse:

    ./dirlogparse -h
    -d int
            -d 5 flag generates debug output (default 4)
    -f string
            Specify input log file name via -f switch
    -o string
            -o /tmp generates output in /tmp.  Default is current directory (default ".")
    -e string
	    -e sjis  (Specify for Japanese Shift-JIS log files only. Do not specify this switch for any other encoding)

Input file:
    Only processes a single file as input.
    If there is a need to process multiple files, concatenate the files in the correct order, and use that as input
    It is assumed that the default/shipped log4j format is retained.  Changes to the date and other log4j formatting are not supported

In case of any questions, contact raja.marla@sas.com.  Constructive criticism and enhancement ideas are welcome.  For assistance with troubleshooting, please include the log file.
