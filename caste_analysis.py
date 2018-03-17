# -*- coding: utf-8 -*-
"""
Created on Mon Jan 22 16:02:33 2018

@author: theism
"""

import os
import gen_func as gf
import pandas as pd
import re
import logging

# define directories
data_dir = r'C:\Users\theism\Documents\Dimagi\Data\case_types\household'
output_dir = r'C:\Users\theism\Documents\Dimagi\Results\Caste\Test'

# refresh locations?
refresh_locations = False

# what file type in folders to get
case_data_regex = re.compile(r'Cases_\d\d\d.csv')
date_cols = []
cols_to_use = ['owner_id', 'closed','hh_bpl_apl','hh_caste','hh_minority','hh_religion']
data_cols = ['hh_bpl_apl','hh_caste','hh_minority','hh_religion']
location_columns = ['doc_id', 'block_name', 'district_name', 'state_name']
real_state_list = ['Madhya Pradesh', 'Chhattisgarh', 'Andhra Pradesh', 'Bihar',
                   'Jharkhand', 'Rajasthan']
# , 'Uttar Pradesh', 'Maharashtra']

# start logging
gf.start_logging(output_dir)

# initialize dfs
input_df = pd.DataFrame()
input_df = input_df.fillna('')
input_df = gf.csv_files_to_df(data_dir, case_data_regex, date_cols, cols_to_use)
    
# only keep open cases
case_df = input_df[input_df['closed'] == False]

# get latest location fixture and add location data
if refresh_locations:
    gf.refresh_locations()
case_df = gf.add_locations(case_df, 'owner_id', location_columns)
case_df = case_df.loc[(case_df['state_name'].isin(real_state_list))]

# get caste percentages to df for output
drop_cols = cols_to_use
drop_cols.append('state_name')
drop_cols.append('district_name')
blocks = case_df['block_name'].unique().tolist()
new_loc_cols = ['block_name', 'district_name', 'state_name']
denom = case_df['hh_caste'].count()

for item in data_cols:
    logging.info('Going through %s' % item)
    loc_df = case_df.drop(drop_cols, axis=1).drop_duplicates('block_name').set_index('block_name')
    category = case_df[item].value_counts().index.tolist()
    for cat in category:
        loc_df[cat] = 0
    for block in blocks:
        loc_df.loc[block] = case_df[case_df['block_name'] == block][item].value_counts()
    loc_df = loc_df.fillna(0)
    loc_df['Total'] = loc_df.sum(axis=1)
    loc_df.loc['All Blocks'] = loc_df.sum()
    for cat in category:
        loc_df[cat + '%'] = loc_df[cat] / loc_df['Total']
    loc_df = gf.add_locations(loc_df, None, new_loc_cols)
    loc_df.to_csv(os.path.join(output_dir,item + '.csv'))
    logging.info(loc_df.loc['All Blocks'])
    
