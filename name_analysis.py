# -*- coding: utf-8 -*-
"""
Created on Sat Sep 09 18:01:46 2017

@author: theism
"""

import gen_func
import case_func
import os
import re
import pandas as pd
import logging
import time

# ----------------  USER EDITS -------------------------------
# Specify data location, regex to use on files, date columns that are dates
# Future - consider a GUI to do this input better

target_dir = (r'C:\Users\theism\Documents\Dimagi\Data\Case2')
output_dir = (r'C:\Users\theism\Documents\Dimagi\Results\Name')

# Practice Use Case on small dataset
# target_dir = (r'C:\Users\theism\Documents\Dimagi\Data\case_testing')
# output_dir = (r'C:\Users\theism\Documents\Dimagi\Results\Name\testing')
# ------------- don't edit below here -----------------------------

# start logging
gen_func.start_logging(output_dir)

# identify all the states files to go through
folder_list = os.listdir(target_dir)
case_date_cols = ['opened_date', 'last_modified_date', 'dob']
case_data_regex = re.compile(r'cases_\d\d\d.csv')

# initialize outputs
output_dict = {}
output_df = pd.DataFrame()
output_df = output_df.fillna('')

logging.info('Looking in folders for data...')
logging.info('Starting scripts to analyze case data...')
for folder in folder_list:
    # initialize dataframe for output errors
    bad_df = pd.DataFrame()
    bad_df = bad_df.fillna('')

    if os.path.isdir(os.path.join(target_dir, folder)):
        location_name = gen_func.folder_name_to_location(folder)
        logging.info('-------------------------------------------')
        logging.info('Going through data for: %s' % location_name)
        logging.info('-------------------------------------------')
        logging.info(time.strftime('%X %x'))
        output_dict = {'location': location_name}

        # combine all csv into one dataframe
        case_df = gen_func.csv_files_to_df(os.path.join(target_dir, folder),
                                           case_data_regex, case_date_cols)
        os.chdir(output_dir)

        # clean cases-closed, orphan, test states/names, awc owner, blank names
        # also gives names with partial of all numeric digits
        case_clean_df, output_dict = case_func.clean_case_data(case_df,
                                                               output_dict)

        # check for blank / skipped name
        good_df = case_func.value_is_blank(case_clean_df, 'name')

        # get number names that aren't same as aadhar dobs
        num_null_raw_aadhar, null_raw_df, has_raw_df = case_func.string_not_null(
            good_df, 'raw_aadhar_string', 'no_raw_aadhar_string')
        num_invalid_2dbarcodes, invalid_2dbarcode_df, invalid_2dbarcode_index = case_func.string_not_contains(
            has_raw_df, 'raw_aadhar_string', 'uid=', 'invalid_2dbarcode_scan', index_out=True)
        good_df_w_aad = has_raw_df[~invalid_2dbarcode_index]
        good_df_w_aad.loc[:, ('aadhar_name')] = good_df_w_aad['raw_aadhar_string'].str.extract('(?<=name=")(\w*\s*\w*)', expand=False)
        name_match_index, same_name = case_func.compare_values(good_df_w_aad, 'aadhar_name', 'name')
        num_w_2dscan = len(good_df_w_aad.index)
        logging.info('%i cases have 2d barcode scans' % num_w_2dscan)
        num_w_aadhar_name = good_df_w_aad['aadhar_name'].notnull().value_counts()[True]
        logging.info('%i of cases w/ 2d scans have \'aadhar_name\' (%i pct)'
                     % (num_w_aadhar_name, (100 * num_w_aadhar_name / num_w_2dscan)))
        logging.info('%i of these have aadhar name match entered name (%i pct)'
                     % (same_name, (100 * same_name / num_w_aadhar_name)))
        num_some_num_w_aad, some_num_df_aad, some_num_index_aad = case_func.string_contains(
                good_df_w_aad, 'aadhar_name', '\d+', 'some numeric chars in aadhar name', index_out=True)
        
        # look at non-matching names a bit
        non_match_df = good_df_w_aad[~name_match_index]
        non_match_df['name_ascii'] = non_match_df['name'].apply(case_func.is_english)
        num_name_chg_from_ascii = non_match_df['name_ascii'].value_counts()[False]
        logging.info('%i names changed from ascii aadhar scanned name to '
                     'non-ascii name (%i pct of scans)'
                     % (num_name_chg_from_ascii,
                        (100 * num_name_chg_from_ascii / len(good_df_w_aad.index))))

        # see how many western chars in names
        num_ascii_only, ascii_df, has_ascii_index = case_func.test_for_english(
            good_df, 'name')
        logging.info('%i names with only ascii(roman) characters (%i pct of %i good clean cases)'
                     % (num_ascii_only, (100 * num_ascii_only / len(good_df.index)), len(good_df.index)))
        
        # see how many that are western only are from aadhar card
        num_ascii_aad_only, ascii_aad_df, has_ascii_aad_index = case_func.test_for_english(
            good_df_w_aad, 'name')
        logging.info('%i names with only ascii(roman) characters that also had'
                     ' an aadhaar card scanned (%i pct of %i aadhar scans)'
                     % (num_ascii_aad_only, (100 * num_ascii_aad_only / num_w_aadhar_name),
                        num_w_aadhar_name))
        
        num_ascii_aad_only2, ascii_aad_df2, has_ascii_aad_index2 = case_func.test_for_english(
            good_df_w_aad, 'aadhar_name')
        logging.info('%i aadhar names with only ascii(roman) characters (%i pct)'
                     % (num_ascii_aad_only2, (100 * num_ascii_aad_only2 / len(good_df_w_aad.index))))
        
        # number of users with 'test' in the name
        num_test_users = good_df[good_df['name'].str.contains('test')].shape[0]
        logging.info('%i with _test_ in username removed' % num_test_users)
        
        # take a look at duplicate names
        num_unique = good_df['name'].nunique()
        logging.info('%i unique names (%i percent out of %i)' % (num_unique,
                     (100 * num_unique / len(good_df.index)), len(good_df.index)))
        logging.info('%i dups and %i percent dups'
                     % (len(good_df.index) - num_unique,
                        ((100 * (len(good_df.index) - num_unique)) / len(good_df.index))))
        name_value_counts = good_df['name'].value_counts()
        logging.info('Top 15 duplicate names:')
        logging.info(name_value_counts.head(15))
