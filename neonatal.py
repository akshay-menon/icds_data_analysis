# -*- coding: utf-8 -*-
"""
Created on 22 June 2018

@author: bderenzi
"""

import re
import os
import pandas as pd
import numpy as np
import gen_func as gf
import logging

# ----------------  USER EDITS -------------------------------

# input directories and information
# data folder format - daily export name, date zip compiled on hq
data_dir = os.path.join(gf.DATA_DIR, '/forms/[DA] Delivery - with children 31may18')
data_regex = re.compile(r'Forms_\d\d\d.csv')
output_dir = os.path.join(gf.OUTPUT_DIR, '/Neonatal outcomes')

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
    'form.how_many_children',
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
have_del_df = have_del_df.rename(
    columns={
        'form.how_many_children': 'total_children',
        'form.total_children_died': 'total_children_died',
    })
logging.info('forms after only-most-recent: %i' % have_del_df.shape[0])

###
# data cleaning!
# there is one row where there is a NaN answer for how many children
have_del_df = have_del_df[have_del_df['total_children'].notna()]

have_del_df['total_children'] = have_del_df[
    'total_children'].astype('int64')

full_set = have_del_df.groupby(locations) \
                        .sum() \
                        .reset_index()
full_set['total_children'] = full_set['total_children'].astype(
    'int64')
full_set['total_children_died'] = full_set[
    'total_children_died'].astype('int64')
full_set[
    'neonatal_mortality_block'] = 100 * (full_set['total_children_died'] / full_set['total_children'])

district = full_set.groupby('district_name').sum()
district[
    'neonatal_mortality_district'] = 100 * (district['total_children_died'] / district['total_children'])
district = district[['neonatal_mortality_district']]

state = full_set.groupby('state_name').sum()
state[
    'neonatal_mortality_state'] = 100 * (state['total_children_died'] / state['total_children'])
state = state[['neonatal_mortality_state']]

full_set = full_set.merge(district, left_on='district_name', right_index=True) \
                    .merge(state, left_on='state_name', right_index=True)

full_set[
    'delta_from_district'] = full_set['neonatal_mortality_district'] - full_set['neonatal_mortality_block']
full_set[
    'delta_from_state'] = full_set['neonatal_mortality_state'] - full_set['neonatal_mortality_block']

full_set.to_csv('overal_neonatal_mortality.csv')
