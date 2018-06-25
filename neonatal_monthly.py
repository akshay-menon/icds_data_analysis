# -*- coding: utf-8 -*-
"""
Created on 22 June 2018

@author: bderenzi
"""

import re
import os
import pandas as pd
import gen_func as gf
import logging

# ----------------  USER EDITS -------------------------------

# input directories and information
# data folder format - daily export name, date zip compiled on hq
data_dir = gf.DATA_DIR + '/forms/[DA] Delivery - with children 31may18'
data_regex = re.compile(r'Forms_\d\d\d.csv')
output_dir = gf.OUTPUT_DIR + '/Neonatal outcomes'

# states we care about for this analysis (used to get rid of test users)
real_state_list = [
    'Madhya Pradesh', 'Chhattisgarh', 'Andhra Pradesh', 'Bihar', 'Jharkhand',
    'Rajasthan'
]
#, 'Uttar Pradesh', 'Maharashtra']

# if datasets get too big, can only choose certain variables to pull from the form
indicator_list = [
    'formid',
    'username',
    'form.has_delivered',
    # 'form.where_born',
    # 'form.which_hospital',
    # 'form.delivery_nature',
    'completed_time',
    'form.total_children_died',
    # 'form.how_many_children',
    'form.case_load_ccs_record0.case.@case_id'
]
date_fmt_cols = ['completed_time']
locations = ['block_name', 'district_name', 'state_name']
## ------------- don't edit below here -----------------------------

# start logging
gf.start_logging(output_dir)
os.chdir(output_dir)

# combine forms into single dataset
input_df = gf.csv_files_to_df(
    data_dir, data_regex, date_cols=date_fmt_cols, cols_to_use=indicator_list)

# process
input_df = input_df.rename(
    columns={'form.case_load_ccs_record0.case.@case_id': 'caseid'})
logging.info('raw forms: %i' % input_df.shape[0])
input_df = gf.filter_by_start_date(input_df, column='completed_time')
input_df = gf.add_locations_by_username(input_df)

del_df = input_df.loc[(input_df['state_name'].isin(real_state_list))]
num_forms = del_df.shape[0]
logging.info('forms after locations and date filter: %i' % num_forms)

# Just to be explicit with Pandas
have_del_df = gf.only_most_recent_deliveries(del_df, column='completed_time')
logging.info('forms after only-most-recent: %i' % have_del_df.shape[0])
# First group-by location, then resample the datetime and sum the columns accordingly.
# Missing values get a zero
full_set = have_del_df.groupby(locations + [pd.Grouper(key='completed_time', freq='M')]) \
                        .sum() \
                        .unstack() \
                        .fillna(0)
full_set['form.total_children_died'].to_csv('monthly_total_neonatal_deaths.csv')