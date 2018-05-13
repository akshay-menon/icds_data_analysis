# -*- coding: utf-8 -*-
"""
Created on Sat Jan 27 11:04:28 2018

@author: theism
"""

import os
import pandas as pd
import gen_func as gf
import re
import logging

data_dir = r'C:\Users\theism\Downloads\[DA] Post Natal Care\[DA] Post Natal Care'
data_regex = re.compile(r'Forms_\d\d\d.csv')
output_name = 'combined_file.csv'
date_cols = ['form.add', 'form.case_load_ccs_record0.case.@date_modified',
             'form.pnc1_date', 'form.pnc2_date', 'form.pnc3_date', 'form.pnc4_date']
cols_to_use = ['userID', 'form.case_load_ccs_record0.case.close',
               'form.safe','form.mother_alive_check',
               'form.days_visit_late',
               'form.pnc1_date', 'form.pnc2_date', 'form.pnc3_date', 'form.pnc4_date',
               'form.case_load_ccs_record0.case.update.num_pnc_visits',
               'form.case_load_ccs_record0.case.update.num_pnc_sched_visits',
               'form.add', 'form.case_load_ccs_record0.case.@date_modified']
location_columns = ['doc_id', 'block_name', 'district_name', 'state_name']
real_state_list = ['Madhya Pradesh', 'Chhattisgarh', 'Andhra Pradesh', 'Bihar',
                   'Jharkhand', 'Rajasthan']
# , 'Uttar Pradesh', 'Maharashtra']

output_df = pd.DataFrame()
output_df = output_df.fillna('')
file_list = gf.data_file_list(data_dir, data_regex)
gf.start_logging(data_dir)

# combine each csv, with only columns necessary, into single dataframe
for data_file in file_list:
    # get data
    logging.info('going through %s' % data_file)
    input_df = pd.read_csv(os.path.join(data_dir, data_file),
                           usecols=cols_to_use, parse_dates=date_cols,
                           infer_datetime_format=True)
    output_df = output_df.append(input_df)
    print(output_df.shape)
    
# add location in order to get rid of test locations
output_df = gf.add_locations(output_df, 'userID', location_columns)
output_df = output_df.loc[(output_df['state_name'].isin(real_state_list))]

# perform calcs interested in
output_df['days_since_add'] = output_df['form.case_load_ccs_record0.case.@date_modified'] - output_df['form.add']
    


    
output_df.to_csv(os.path.join(data_dir, output_name))
logging.info('all files combined, output saved to directory')
    
    