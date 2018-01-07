# icds_data_analysis

This text doc explains the structure of the python scripts being created to analyze data for ICDS-CAS.

Note - These scripts reflect local paths to data downloaded and stored on the laptop used for the requisite analysis.  You may run into errors if you run the scripts and haven’t corrected the paths to data files, directories for log files, or location fixtures used to map awc/block/district/state to a given user or case.

Scripts are written using Python 2.7

To use these scripts, search for the analysis or the name of the script in question to see a basic overview.  In general, there are supporting scripts with functions, either generic or analysis specific.  There are main scripts to run the analysis that call the supporting scripts.  In the main scripts, there is a ‘user edit’ section at the top to specify paths to data folders for input data and output results.  Generally, there are two sets, one for a small test dataset and one for a full analysis.  One is always commented out.

Generic scripts:
gen_func.py - used for high level functions that are used across numerous scripts.  These are mainly file manipulation or extremely general functions.
case_func.py - used for case level analysis.  These functions are used in the analysis of case exports and case data.  They may contain some functions that may be useful for any analysis, however, as the library of functions grows as more analysis are conducted.
form_func.py - used for form data analysis.  These functions are formatted for analysis of form data rather than case data.

Aadhar scripts - used in the Aadhar Number Analysis
aadhar_func.py - supporting scripts for the analysis of aadhar numbers for person cases
verhoeff.py - script that contains the algorithms for the verhoeff checksum that aadhar numbers employ
aadhar_number_analysis.py - the main script to run to analyze aadhar numbers.  References aadhar_func, gen_func and case_func, as well as a few other generic python libraries.  Update the information in the ‘user edits’ section at the top of the script to adapt to your machine.

Phone number scripts - used in the User Phone Number Analysis and the Case Phone Number Analysis:
phone_func.py - supporting scripts for the analysis of phone numbers for person cases and user cases
phone_number_analysis.py - the main script to run to analyze phone numbers.  References phone_func, gen_func and case_func, as well as a few other generic python libraries.  Update the information in the ‘user edits’ section at the top of the script to adapt to your machine, and whether to point at user case or person case phone numbers.

Name scripts - used in the Name Analysis:
name_analysis.py - main script to run to analyze case names.

Date of Birth scripts- used in the Date of Birth Analysis:
date_of_birth.py - main script to run to analyze case dates of birth.

Form scripts - used in Form Lag, User Activity, and Submission by Type analyses:
form_lag.py - for Form Lag analysis
form_activity.py - for user activity analysis
forms_by_type.py - for form submission by type analysis
