# -*- coding: utf-8 -*-
"""
Created on Fri Sep 01 07:07:53 2017

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
output_dir = (r'C:\Users\theism\Documents\Dimagi\Results\Date of Birth')

# Practice Use Case on small dataset
#target_dir = (r'C:\Users\theism\Documents\Dimagi\Data\case_testing')
#output_dir = (r'C:\Users\theism\Documents\Dimagi\Results\Date of Birth\testing')
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
        output_dict = {'location': location_name}
        logging.info(time.strftime('%X %x'))

        # combine all csv into one dataframe
        case_df = gen_func.csv_files_to_df(os.path.join(target_dir, folder),
                                           case_data_regex, case_date_cols)
        os.chdir(output_dir)

        # clean cases-closed, orphan, test states/names, awc owner, blank names
        case_clean_df, output_dict = case_func.clean_case_data(case_df,
                                                               output_dict)

        # add age distribution information to dataframe
        case_clean_df = case_func.add_age_info(case_clean_df,
                                               col_name='big_age_bracket',
                                               bin_type='brackets',
                                               relative_date='opened_date')

        # check for blank / skipped dob
        good_df = case_func.value_is_blank(case_clean_df, 'dob')

        # compare dob column to date opened column - make sure same dtype
        good_df['dob'] = pd.to_datetime(good_df['dob'], infer_datetime_format=True, errors='coerce')
        #good_df['opened_date'] = pd.to_datetime(good_df['opened_date'].dt.normalize, infer_datetime_format=True, errors='coerce')
        # and get rid of times in opened_date - just keep date and midnight time
        good_df['opened_date'] = good_df['opened_date'].dt.normalize()
        dob_is_open_index = case_func.compare_values(good_df, 'opened_date', 'dob')
        logging.info('WARNING - this number could have valid for cases that'
                     'are newborns included, but not detecting them')

        # get number dob's that aren't same as aadhar dobs
        num_null_raw_aadhar, null_raw_df, has_raw_df = case_func.string_not_null(
            good_df, 'raw_aadhar_string', 'no_raw_aadhar_string')
        num_invalid_2dbarcodes, invalid_2dbarcode_df, invalid_2dbarcode_index = case_func.string_not_contains(
            has_raw_df, 'raw_aadhar_string', 'uid=', 'invalid_2dbarcode_scan', index_out=True)
        good_df_w_aad = has_raw_df[~invalid_2dbarcode_index]
        good_df_w_aad.loc[:, ('aadhar_yob')] = good_df_w_aad['raw_aadhar_string'].str.extract('(?<=yob=")(\d+)', expand=False)
        # note - dob format in aadhar not consistent.  some 18/09/1980, some 2000-08-20
        good_df_w_aad.loc[:, ('aadhar_dob')] = good_df_w_aad['raw_aadhar_string'].str.extract('(?<=dob=")(\d+.\d+.\d+)', expand=False)
        good_df_w_aad.loc[:, ('aadhar_dob')] = pd.to_datetime(good_df_w_aad['aadhar_dob'], errors='coerce')
        dob_match_index, same_dob = case_func.compare_values(good_df_w_aad, 'aadhar_dob', 'dob')
        good_df_w_aad.loc[:, ('dob_year')] = good_df_w_aad['dob'].apply(lambda x: str(x.year))
        yob_match_index, same_yob = case_func.compare_values(good_df_w_aad, 'dob_year', 'aadhar_yob')
        num_w_2dscan = len(good_df_w_aad.index)
        logging.info('%i cases have 2d barcode scans' % num_w_2dscan)
        num_w_aadhar_dob = good_df_w_aad['aadhar_dob'].notnull().value_counts()[True]
        logging.info('%i of cases w/ 2d scans have \'dob\' (%i pct)'
                     % (num_w_aadhar_dob, (100 * num_w_aadhar_dob / num_w_2dscan)))
        logging.info('%i of these have aadhar dob match entered dob (%i pct)'
                     % (same_dob, (100 * same_dob / num_w_aadhar_dob)))
        num_w_aadhar_yob = good_df_w_aad['aadhar_yob'].notnull().value_counts()[True]
        logging.info('%i of cases w/ 2d scans have \'yob\' (%i pct)'
                     % (num_w_aadhar_yob, (100 * num_w_aadhar_yob / num_w_2dscan)))
        logging.info('%i of these with aadhar yob match year in entered dob (%i pct)'
                     % (same_yob, (100 * same_yob / num_w_aadhar_yob)))
        
        # see distribution of population by age and gender
        # by gender
        logging.info('Distributions of ALL CLEAN cases by gender and age...')
        sex_counts = case_clean_df['sex'].value_counts(dropna=False)
        sex = pd.DataFrame({'count': sex_counts,
                            'pct': sex_counts / len(case_clean_df)})
        sex.loc['Total'] = sex.sum()
        logging.info('Value counts of cases by sex:')
        logging.info(sex)
        logging.info('ErrChk: %i table sum, %i total, %i diff'
                     % (sex.loc['Total']['count'], len(case_clean_df.index),
                        len(case_clean_df.index) - sex.loc['Total']['count']))
        # by age (big buckets)
        age_counts = case_clean_df['big_age_bracket'].value_counts(dropna=False)
        age = pd.DataFrame({'count': age_counts,
                            'pct': age_counts / len(case_clean_df)})
        # age.loc['Total'] = age.sum()
        logging.info('Value counts of cases by age bucket:')
        logging.info(age)
        logging.info('ErrChk: %i table sum, %i total, %i diff'
                     % (age.sum()[0], len(case_clean_df.index),
                        len(case_clean_df.index) - age.sum()[0]))

        # by age/gender
        # convert via .astype(str) to string before grouping. That will conserve the NaN's.
        age_sex_counts = case_clean_df.groupby(['big_age_bracket', 'sex']).count()['caseid']
        age_sex = pd.DataFrame({'count': age_sex_counts,
                                'pct': age_sex_counts / len(case_clean_df)})
        # age_sex.loc['Total'] = age_sex.sum()
        logging.info('Distribution by age/sex:')
        logging.info(age_sex)
        logging.info('ErrChk: %i table sum, %i total, %i diff'
                     % (age_sex.sum()[0], len(case_clean_df.index),
                        len(case_clean_df.index) - age_sex.sum()[0]))
