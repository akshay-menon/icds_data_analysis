# -*- coding: utf-8 -*-
"""
Created on Fri May 25 07:49:23 2018

@author: theism / bderenzi
"""

import re
import os
import pandas as pd
import gen_func as gf
import logging

# ----------------  USER EDITS -------------------------------

# input directories and information
# data folder format - daily export name, date zip compiled on hq
data_dir = gf.DATA_DIR + '/forms/[DA] Delivery - 30apr18'
data_regex = re.compile(r'Forms_\d\d\d.csv')
output_dir = gf.OUTPUT_DIR + '/Delivery forms'

# states we care about for this analysis (used to get rid of test users)
real_state_list = ['Madhya Pradesh', 'Chhattisgarh',
                   'Andhra Pradesh', 'Bihar', 'Jharkhand',
                   'Rajasthan']
#, 'Uttar Pradesh', 'Maharashtra']

# if datasets get too big, can only choose certain variables to pull from the form
indicator_list = ['formid', 'username', 'form.has_delivered', 'form.where_born', 'form.which_hospital', 'form.delivery_nature', 'completed_time']
date_fmt_cols = ['completed_time']

locations = ['block_name', 'district_name', 'state_name']
## ------------- don't edit below here -----------------------------

# start logging
gf.start_logging(output_dir)
os.chdir(output_dir)

# combine forms into single dataset
input_df = gf.forms_to_df(data_dir, data_regex, date_cols=date_fmt_cols, cols_to_use=indicator_list)
logging.info('raw forms: %i' % input_df.shape[0])
input_df = gf.add_locations_by_username(input_df)
logging.info('raw forms after add locations: %i' % input_df.shape[0])

# filter out forms from states dont want in analysis
del_df = input_df.loc[(input_df['state_name'].isin(real_state_list))]
num_forms = del_df.shape[0]
# logging.info('Num forms in real locations: %i' % num_forms)
# logging.info('%i different users submitted this form' % del_df['awc_name'].nunique())
# logging.info('%.2f average forms per user' % del_df['awc_name'].value_counts().mean())

# logging.info(del_df['form.has_delivered'].value_counts(dropna=False))
# Just to be explicit with Pandas
have_del_df = del_df[del_df['form.has_delivered'] == 'yes'].copy(False)
# logging.info(have_del_df['form.where_born'].value_counts(dropna=False))
# logging.info(have_del_df['form.delivery_nature'].value_counts(dropna=False))
have_del_df['home_delivery'] = (have_del_df['form.where_born'] == 'home')
have_del_df['transit_delivery'] = (have_del_df['form.where_born'] == 'transit')
have_del_df['hospital_delivery'] = (have_del_df['form.where_born'] == 'hospital')
have_del_df['caesarean_delivery'] = (have_del_df['form.delivery_nature'] == 'caesarean')
have_del_df['delivery'] = (have_del_df['form.has_delivered'] == 'yes')

# First group-by location, then resample the datetime and sum the columns accordingly.
# Missing values get a zero
full_set = have_del_df.groupby(locations).resample('M', on='completed_time').sum().unstack().fillna(0)
full_set['home_delivery'].reset_index().to_csv('monthly_home_deliveries.csv')
full_set['hospital_delivery'].reset_index().to_csv('monthly_hospital_deliveries.csv')
full_set['caesarean_delivery'].reset_index().to_csv('monthly_caesarean_deliveries.csv')

full_set['delivery'].reset_index().to_csv('monthly_total_deliveries.csv')
