# icds_data_analysis

This text doc explains the structure of the python scripts being created to analyze data for ICDS-CAS.

Note - These scripts reflect local paths to data downloaded and stored on the laptop used for the requisite analysis.  You may run into errors if you run the scripts and haven’t corrected the paths to data files, directories for log files, or location fixtures used to map awc/block/district/state to a given user or case.

We are working to replace the hardcoded paths. To do that, we have created a settings.py file that is not stored in the repository. A sample settings file is made available for using relative directories.

Scripts are written using Python 2.7, though we are working to make them compatible with Python 2.7 and Python 3.

To use these scripts, search for the analysis or the name of the script in question to see a basic overview.  In general, there are supporting scripts with functions, either generic or analysis specific.  There are main scripts to run the analysis that call the supporting scripts.  In the main scripts, there is a ‘user edit’ section at the top to specify paths to data folders for input data and output results.  In some scripts, there are two sets of paths, one for a small test dataset and one for a full analysis, one of which is always commented out.

Generic scripts:
gen_func.py - used for high level functions that are used across numerous scripts.  These are mainly file manipulation or extremely general functions.
case_func.py - used for case level analysis.  These functions are used in the analysis of case exports and case data.  They may contain some functions that may be useful for any analysis, however, as the library of functions grows as more analysis are conducted.
form_func.py - used for form data analysis.  These functions are formatted for analysis of form data rather than case data.

To get started:
1. Download python 2.7.  This can be done with lots of the appropriate libraries already installed from https://www.anaconda.com/download/
2. Clone this repository onto your computer.  If you aren't familiar with how to do that, start here: https://guides.github.com/activities/hello-world/
3. If you create new files or edit files, check them into a branch and create a pull request.
4. Download the data necessary to run the script.  Often this is detailed in the work log accompanying an analysis in the ICDS-CAS Data Analysis google drive folder.
5. Run the script!

Some of the initial analyses are described here to give example structure.

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
