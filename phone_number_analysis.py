# -*- coding: utf-8 -*-
"""
Phone Number Analysis

Can be used for different case types - edit first few rows

@author: Matt Theis, July 2017
"""
import re
import os
import pandas as pd
import numpy as np
import datetime
import logging
import gen_func
import case_func
import phone_func

# ----------------  USER EDITS -------------------------------
# Specify data location, regex to use on files, columns that are dates

# PERSON CASE PHONE NUMBERS
# target_dir = (r'C:\Users\theism\Documents\Dimagi\Data\Case')
# output_dir = (r'C:\Users\theism\Documents\Dimagi\Results\Person case Phone Data Outputs')
# output_dir = (r'C:\Users\theism\Dropbox (Dimagi)\Dimagi - Projects\Gates Foundation\ICDS Scale Up\Web App - Other (Non-Reporting)\Data\Data Quality\Phone Numbers')
# case_data_regex = re.compile(r'cases_\d\d\d.csv')
#case_date_cols = ['opened_date', 'dob']

target_dir = (r'C:\Users\theism\Documents\Dimagi\Data\Case2')
output_dir = (r'C:\Users\theism\Documents\Dimagi\Results\Person case Phone Data Outputs')
case_data_regex = re.compile(r'cases_\d\d\d.csv')
case_date_cols = ['opened_date', 'dob']

# USER PHONE NUMBERS
# target_dir = (r'C:\Users\theism\Documents\Dimagi\Data\User2')
# output_dir = (r'C:\Users\theism\Documents\Dimagi\Results\User case phone data')
# case_data_regex = re.compile(r'Cases_\d\d\d.csv')
# case_date_cols = []

# Practice Use Case on small dataset
#target_dir = (r'C:\Users\theism\Documents\Dimagi\Data\person_phone_aadhar-ap-anantapur2')
#output_dir = (r'C:\Users\theism\Documents\Dimagi\Data\person_phone_aadhar-ap-anantapur2\test')
#case_data_regex = re.compile(r'cases_\d\d\d.csv')
#case_date_cols = ['opened_date', 'dob']

deep_dive_dups = False
# ------------- don't edit below here -----------------------------

gen_func.start_logging(output_dir)

# identify all the states files to go through
folder_list = os.listdir(target_dir)


# initialize outputs
phone_output_dict = {}
phone_output = pd.DataFrame()
phone_output = phone_output.fillna('')

logging.info('Starting scripts to analyze phone case data...')
for folder in folder_list:
    bad_num_list = pd.DataFrame()
    bad_num_list = bad_num_list.fillna('')

    if os.path.isdir(os.path.join(target_dir, folder)):
        location_name = gen_func.folder_name_to_location(folder)
        logging.info('-------------------------------------------')
        logging.info('Going through data for: %s' % location_name)
        logging.info('-------------------------------------------')
        phone_output_dict = {'location': location_name}

        # combine all csv into one dataframe
        case_df = gen_func.csv_files_to_df(os.path.join(target_dir, folder),
                                           case_data_regex, case_date_cols)

        if location_name == 'User':
            case_df['phone_number'] = np.nan
            case_df['phone_number'] = case_df.apply(
                    lambda row: row['aww_phone_number']
                    if (row['aww_phone_number'] != '---') & (
                            row['aww_phone_number'] != np.nan)
                    else row['ls_phone_number'], axis=1)

        # clean case data
        case_clean_df, phone_output_dict = case_func.clean_case_data(
                case_df, phone_output_dict, awc_test=False)

        if location_name == 'User':
            case_df = gen_func.add_usertype_from_id(case_df, 'commcare_location_id')
            logging.info('User distribution by type for open, non-test user cases:')
            logging.info(case_df['location_type'].value_counts())

        # clean phone specific data
        phone_clean_df, phone_output_dict = phone_func.clean_phone_data(
                case_clean_df, phone_output_dict)
        phone_output_dict, bad_num_list = phone_func.analyze_phone_data(
                phone_clean_df, phone_output_dict, deep_dive_dups)
        bad_num_list.to_csv(os.path.join(target_dir, (
                'bad_num_list_' + location_name + '_' + str(
                        datetime.date.today()) + '.csv')))

        if location_name != 'User':
            num_non_awc_df, non_awc_df = case_func.search_non_awc_owners(case_df)
            non_awc_df.to_csv(os.path.join(output_dir, (
                    'non_awc_owned_cases_' + location_name + '_' + str(
                            datetime.date.today()) + '.csv')))

        # create summary output
        phone_output = phone_output.append(
                phone_output_dict, ignore_index=True)

logging.info('Reformatting and writing output...')
# reformat output
phone_output = phone_output.set_index('location')
phone_output.loc['Total'] = phone_output.sum()
ordered_columns = ['orig_rows',
                   'num_closed',
                   'num_wo_hh_id',
                   'num_blank_name',
                   'non_awc_num',
                   'num_test_locations',
                   'num_test_users',
                   'num_clean_rows',
                   'num_unverified',
                   'num_non_female',
                   'num_out_of_age',
                   'num_clean_phone_nums',
                   'unique_phone',
                   'num_duplicates',
                   'num_91_only',
                   'num_123456789',
                   'num_987654321',
                   'num_mismatch',
                   'num_non_numeric',
                   'num_bad_prefix',
                   'num_bad_lang_code',
                   'num_non_ten_char',
                   'top_dups',
                   'top_dup_counts']

if deep_dive_dups:
    ordered_columns = (
            ordered_columns + ['num_repeated_hh', 'num_all_repeated_hh',
                               'num_repeated_owner', 'num_all_repeated_owner',
                               'unique_df_owner', 'unique_df_hh',
                               'unique_dup_owner', 'unique_dup_hh'])

phone_output = phone_output[ordered_columns]

# insert percentages
phone_output.insert(
        phone_output.columns.get_loc('unique_phone'), 'pct_verified_to_clean',
        (phone_output['num_clean_phone_nums'] * 100 / phone_output['num_clean_rows']))
phone_output.insert(
        phone_output.columns.get_loc('num_duplicates'), 'pct_unique_to_verified',
        (phone_output['unique_phone'] * 100 / phone_output['num_clean_phone_nums']))
phone_output.insert(
        phone_output.columns.get_loc('num_91_only'), 'pct_dup_to_verified',
        (phone_output['num_duplicates'] * 100 / phone_output['num_clean_phone_nums']))

if deep_dive_dups:
    phone_output.insert(
            phone_output.columns.get_loc('unique_df_hh'), 'pct_one_dup_hh',
            (phone_output['num_repeated_hh'] * 100 / phone_output['num_duplicates']))
    phone_output.insert(
            phone_output.columns.get_loc('unique_df_hh'), 'pct_all_dup_hh',
            (phone_output['num_all_repeated_hh'] * 100 / phone_output['num_duplicates']))
    phone_output.insert(
            phone_output.columns.get_loc('unique_df_hh'), 'pct_hh_in_dup',
            (phone_output['unique_dup_hh'] * 100 / phone_output['unique_df_hh']))
    phone_output.insert(
            phone_output.columns.get_loc('unique_dup_owner'), 'pct_one_dup_owner',
            (phone_output['num_repeated_owner'] * 100 / phone_output['num_duplicates']))
    phone_output.insert(
            phone_output.columns.get_loc('unique_dup_owner'), 'pct_all_dup_owner',
            (phone_output['num_all_repeated_owner'] * 100 / phone_output['num_duplicates']))
    phone_output.insert(
            phone_output.columns.get_loc('unique_dup_owner'), 'pct_owner_in_dup',
            (phone_output['unique_dup_owner'] * 100 / phone_output['unique_df_owner']))

phone_output = phone_output.transpose()
phone_output.to_csv(os.path.join(output_dir, (
        'phone_analysis_results_' + str(
                datetime.date.today())+'.csv')), encoding='utf-8-sig')
logging.info('Done writing output.')

logging.info(phone_output)
logging.info('DONE.')
logging.shutdown()
