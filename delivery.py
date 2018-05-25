# -*- coding: utf-8 -*-
"""
Created on Fri May 25 07:49:23 2018

@author: theism
"""

import re
import os
import pandas as pd
import gen_func as gf
import logging

# ----------------  USER EDITS -------------------------------

# input directories and information
# data folder format - daily export name, date zip compiled on hq
data_dir = 'C:\\Users\\theism\\Documents\\Dimagi\\Data\\forms\\[DA] Delivery - 30apr18'
data_regex = re.compile(r'Forms_\d\d\d.csv')
output_dir = 'C:\\Users\\theism\\Documents\\Dimagi\\Results\\Delivery forms'

# states we care about for this analysis (used to get rid of test users)
real_state_list = ['Madhya Pradesh', 'Chhattisgarh',
                   'Andhra Pradesh', 'Bihar', 'Jharkhand',
                   'Rajasthan']
#, 'Uttar Pradesh', 'Maharashtra'] 

# if datasets get too big, can only choose certain variables to pull from the form
indicator_list = ['formid', 'username', 'form.has_delivered', 'form.where_born', 'form.which_hospital', 'form.delivery_nature']
date_fmt_cols = []
## ------------- don't edit below here -----------------------------

# start logging
gf.start_logging(output_dir)
os.chdir(output_dir)

# combine forms into single dataset
input_df = gf.csv_files_to_df(data_dir, data_regex, date_cols=date_fmt_cols, cols_to_use=indicator_list)
logging.info('raw forms: %i' % input_df.shape[0])
input_df = gf.add_locations_by_username(input_df)
logging.info('raw forms after add locations: %i' % input_df.shape[0])

# filter out forms from states dont want in analysis
del_df = input_df.loc[(input_df['state_name'].isin(real_state_list))]
num_forms = del_df.shape[0]
logging.info('Num forms in real locations: %i' % num_forms)
logging.info('%i different users submitted this form' % del_df['awc_name'].nunique())
logging.info('%.2f average forms per user' % del_df['awc_name'].value_counts().mean())

logging.info(del_df['form.has_delivered'].value_counts(dropna=False))
have_del_df = del_df[del_df['form.has_delivered'] == 'yes']
logging.info(have_del_df['form.where_born'].value_counts(dropna=False))
logging.info(have_del_df['form.delivery_nature'].value_counts(dropna=False))
have_del_df['home_delivery'] = (have_del_df['form.where_born'] == 'home')
have_del_df['delivery'] = (have_del_df['form.has_delivered'] == 'yes')

have_del_block_df = pd.DataFrame()
have_del_block_df['number home deliveries in block'] = have_del_df.groupby(['block_name', 'district_name', 'state_name'])['home_delivery'].sum()
have_del_block_df['total deliveries in block'] = have_del_df.groupby(['block_name', 'district_name', 'state_name'])['delivery'].sum()
have_del_block_df.to_csv('block_delivery_test.csv')
        