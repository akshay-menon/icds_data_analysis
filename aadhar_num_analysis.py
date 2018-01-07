# -*- coding: utf-8 -*-
"""
Aadhar Case Prop Analysis

Script to Analyze Aadhar Data

@author: Matt Theis, July 2017
"""
import re
import os
import logging
import datetime
import pandas as pd
import gen_func
import case_func
import aadhar_func

# ----------------  USER EDITS -------------------------------
# Specify data location, regex to use on files, date columns that are dates
# Future - consider a GUI to do this input better

target_dir = (r'C:\Users\theism\Documents\Dimagi\Data\Case2')
output_dir = (r'C:\Users\theism\Documents\Dimagi\Results\Aadhar Numbers2')
case_data_regex = re.compile(r'cases_\d\d\d.csv')

# Practice Use Case on small dataset
#target_dir = (r'C:\Users\theism\Documents\Dimagi\Data\person_phone_aadhar-ap-anantapur2')
#output_dir = (r'C:\Users\theism\Documents\Dimagi\Data\person_phone_aadhar-ap-anantapur2\test')
#case_data_regex = re.compile(r'cases_\d\d\d.csv')

# ------------- don't edit below here -----------------------------

gen_func.start_logging(output_dir)

# identify all the states files to go through
folder_list = os.listdir(target_dir)
case_date_cols = ['opened_date', 'dob']

# initialize outputs
output_dict = {}
output_df = pd.DataFrame()
output_df = output_df.fillna('')

logging.info('Starting scripts to analyze aadhar data...')
for folder in folder_list:
    bad_df = pd.DataFrame()
    bad_df = bad_df.fillna('')

    if os.path.isdir(os.path.join(target_dir, folder)):
        location_name = gen_func.folder_name_to_location(folder)
        logging.info('-------------------------------------------')
        logging.info('Going through data for: %s' % location_name)
        logging.info('-------------------------------------------')
        output_dict = {'location': location_name}

        # combine all csv into one dataframe
        case_df = gen_func.csv_files_to_df(os.path.join(target_dir, folder),
                                           case_data_regex, case_date_cols)

        # clean case data and start to get age distribution information
        case_clean_df, output_dict = case_func.clean_case_data(case_df,
                                                               output_dict)
        case_clean_df = case_func.add_age_info(case_clean_df)
        clean_case_age_dist = case_clean_df.groupby(
                ['age_bracket', 'sex']).count()['caseid']

        # clean phone specific data
        aadhar_clean_df, output_dict = aadhar_func.clean_aadhar_data(
                case_clean_df, output_dict)

        # compile age distribution stats
        logging.info('Value counts of cases with aadhar numbers:')
        logging.info(aadhar_clean_df['sex'].value_counts())
        logging.info(aadhar_clean_df['age_bracket'].value_counts())
        logging.info('Distribution by age/sex table:')
        clean_aadhar_age_dist = aadhar_clean_df.groupby(
                ['age_bracket', 'sex']).count()['caseid']
        age_dist_df = pd.DataFrame({'all_clean_cases': clean_case_age_dist,
                                    'clean_w_aadhar': clean_aadhar_age_dist})
        age_dist_df['pct_w_aadhar'] = (
                age_dist_df['clean_w_aadhar'] / age_dist_df['all_clean_cases'])
        logging.info(age_dist_df)

        # run aadhar specific analysis scripts
        output_dict, bad_df = aadhar_func.analyze_aadhar_data(
                aadhar_clean_df, output_dict)
        bad_df.to_csv(os.path.join(output_dir, (
                'bad_list_' + location_name + '_' + str(
                        datetime.date.today()) + '.csv')))

        # create summary output
        output_df = output_df.append(output_dict, ignore_index=True)

logging.info('Reformatting and writing output...')
# reformat output
output_df = output_df.set_index('location')
output_df.loc['Total'] = output_df.sum()

ordered_columns = ['orig_rows',
                   'num_closed',
                   #'num_wo_hh_id',
                   'num_blank_name',
                   'non_awc_num',
                   'num_test_locations',
                   'num_test_users',
                   'num_clean_rows',
                   'num_blank',
                   'num_skipped',
                   'num_clean_aadhar_nums',
                   'num_unique',
                   'num_duplicates',
                   'num_repeated_twice',
                   'num_99',
                   'num_123456789',
                   'num_987654321',
                   'num_repeat_dig',
                   'num_non_numeric',
                   'num_non_12_char',
                   'num_failed_checksum',
                   'num_good_aadhar',
                   'num_attempted_scan',
                   'num_valid_2d_scans',
                   'num_valid_1d_scans',
                   'num_invalid_scans',
                   'num_mismatch_2d_scan',
                   'num_mismatch_1d_scan',
                   'num_mismatch_bad1d_scan',
                   'num_manually_changed',
                   'top_dups',
                   'top_dup_counts']

output_df = output_df[ordered_columns]

# insert percentages
output_df.insert(output_df.columns.get_loc('num_unique'), 'pct_clean_aadhar_to_clean_cases',
                 (output_df['num_clean_aadhar_nums'] * 100 / output_df['num_clean_rows']))
output_df.insert(output_df.columns.get_loc('num_duplicates'), 'pct_unique_aadhar_to_clean_aadhar',
                 (output_df['num_unique'] * 100 / output_df['num_clean_aadhar_nums']))
output_df.insert(output_df.columns.get_loc('num_99'), 'pct_duplicated_twice_to_duplicated',
                 (output_df['num_repeated_twice'] * 100 / output_df['num_duplicates']))
output_df.insert(output_df.columns.get_loc('num_good_aadhar'), 'pct_failed_checksum_to_clean_aadhar',
                 (output_df['num_failed_checksum'] * 100 / output_df['num_clean_aadhar_nums']))
output_df.insert(output_df.columns.get_loc('num_attempted_scan'), 'pct_good_aadhar_to_clean_aadhar',
                 (output_df['num_good_aadhar'] * 100 / output_df['num_clean_aadhar_nums']))
output_df.insert(output_df.columns.get_loc('num_attempted_scan'), 'pct_attempted_scan_to_clean_aadhar',
                 (output_df['num_attempted_scan'] * 100 / output_df['num_clean_aadhar_nums']))
output_df.insert(output_df.columns.get_loc('num_manually_changed'), 'pct_invalid_scans_to_attempted_scans',
                 (output_df['num_invalid_scans'] * 100 / output_df['num_attempted_scan']))
output_df.insert(output_df.columns.get_loc('top_dups'), 'pct_manually_changed_to_attempted_scans',
                 (output_df['num_manually_changed'] * 100 / output_df['num_attempted_scan']))

output_df = output_df.transpose()
output_df.to_csv(os.path.join(output_dir, ('analysis_results_' + str(
        datetime.date.today())+'.csv')), encoding='utf-8-sig')
logging.info('Done writing output.')

logging.info(output_df)
logging.info('DONE.')
logging.shutdown()
