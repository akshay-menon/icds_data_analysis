# -*- coding: utf-8 -*-
"""
Created on Thu Nov 30 19:54:14 2017

@author: theism
"""
import gen_func
import case_func
import logging
import re

# ----------------  USER EDITS -------------------------------
# Specify data location, regex to use on files, date columns that are dates
# Future - consider a GUI to do this input better

#target_dir = (r'C:\Users\theism\Documents\Dimagi\Data\Case3\person_phone_aadhar-mp')
target_dir = (r'C:\Users\theism\Documents\Dimagi\Data\Case3\person_phone_aadhar-ch')
#target_dir = (r'C:\Users\theism\Documents\Dimagi\Data\Case3\person_phone_aadhar-ap')
#target_dir = (r'C:\Users\theism\Documents\Dimagi\Data\Case3\person_phone_aadhar-bihar')
output_dir = (r'C:\Users\theism\Documents\Dimagi\Results\RCH')
#case_data_regex = re.compile(r'\d+_Cases.csv')
case_data_regex = re.compile(r'cases_\d\d\d.csv')
case_date_cols = ['opened_date', 'dob']
cols_to_use = ['has_rch', 'rch_id', 'closed', 'owner_id', 'opened_date', 'dob', 'sex', 'caseid']
real_state_list = ['Madhya Pradesh', 'Chhattisgarh',
                   'Andhra Pradesh', 'Bihar', 'Jharkhand',
                   'Rajasthan']

# Practice Use Case on small dataset
#target_dir = (r'C:\Users\theism\Documents\Dimagi\Data\person_phone_aadhar-ap-anantapur2')
#output_dir = (r'C:\Users\theism\Documents\Dimagi\Data\person_phone_aadhar-ap-anantapur2\test')
#case_data_regex = re.compile(r'cases_\d\d\d.csv')

# ------------- don't edit below here -----------------------------

gen_func.start_logging(output_dir)

logging.info('Starting scripts to analyze aadhar data...')

# combine all csv into one dataframe
case_df = gen_func.csv_files_to_df(target_dir,
                                   case_data_regex,
                                   case_date_cols,
                                   cols_to_use)
    
# clean case data and start to get age distribution information
output_dict = {}
case_clean_df, output_dict = case_func.clean_case_data(case_df, output_dict)
case_clean_df = case_func.add_age_info(case_clean_df)
location_column_names = ['doc_id', 'district_name']
case_clean_df = gen_func.add_locations(case_clean_df, 'owner_id', location_column_names)
case_clean_df = case_clean_df.loc[(case_clean_df['state_name'].isin(real_state_list))]

logging.info(case_clean_df['sex'].value_counts())
logging.info(case_clean_df['age_bracket'].value_counts())
clean_case_age_dist = case_clean_df.groupby(
        ['age_bracket', 'sex']).count()['caseid']
logging.info(clean_case_age_dist)
#case_clean_df = case_clean_df[(case_clean_df['district_name'] == 'WestGodavari')]
logging.info('------ FEMALE 15-49 -----------------')
target_clean_df = case_clean_df[(case_clean_df['sex'] == 'F') & (case_clean_df['age_bracket'] == '15-49 yrs')]
logging.info(target_clean_df['sex'].value_counts())
logging.info(target_clean_df['age_bracket'].value_counts())
logging.info(target_clean_df['has_rch'].value_counts())

skipped_index = target_clean_df['rch_id'] == '---'
num_skipped = skipped_index.value_counts()[True]
good_rch = target_clean_df[~skipped_index]
logging.info('%i cases with skipped rch id found' % num_skipped)

blank_index = good_rch['rch_id'].isnull()
num_blank = blank_index.value_counts()[True]
good_rch = good_rch[~skipped_index]
logging.info('%i cases with blank rch id found' % num_blank)

value_counts = good_rch['rch_id'].value_counts()
duplicate_series = value_counts[value_counts != 1]
logging.info('unique rch id')
logging.info(good_rch['rch_id'].nunique())
logging.info(value_counts.head())

logging.info('------ CHILDREN 0-5 yrs -----------------')
test_child_df = case_clean_df[case_clean_df['age_bracket'] == '0-5 yrs']
logging.info(test_child_df['sex'].value_counts())
logging.info(test_child_df['age_bracket'].value_counts())
logging.info(test_child_df['has_rch'].value_counts())

skipped_index = test_child_df['rch_id'] == '---'
num_skipped = skipped_index.value_counts()[True]
good_child_rch = test_child_df[~skipped_index]
logging.info('%i cases with skipped rch id found' % num_skipped)

blank_index = good_child_rch['rch_id'].isnull()
num_blank = blank_index.value_counts()[True]
good_child_rch = good_child_rch[~skipped_index]
logging.info('%i cases with blank rch id found' % num_blank)

child_value_counts = good_child_rch['rch_id'].value_counts()
duplicate_series = child_value_counts[child_value_counts != 1]
logging.info('unique rch id')
logging.info(good_child_rch['rch_id'].nunique())
logging.info(child_value_counts.head())
