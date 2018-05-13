# -*- coding: utf-8 -*-
"""
Created on Fri May 04 15:25:04 2018

@author: theism
"""

# See the usage of AGMP forms outside of the linkage to GMP

import re
import os
import pandas as pd
import gen_func as gf
import logging

# ----------------  USER EDITS -------------------------------

# input directories and information
gmp_dir = r'C:\Users\theism\Documents\Dimagi\Data\growth_monitoring_forms'
agmp_dir = r'C:\Users\theism\Documents\Dimagi\Data\new_forms\Additional Growth Monitoring'
data_regex = re.compile(r'Forms_\d\d\d.csv')
output_dir = 'C:\\Users\\theism\\Documents\\Dimagi\\Results\\AGMP'

# states we care about
real_state_list = ['Madhya Pradesh', 'Chhattisgarh',
                   'Andhra Pradesh', 'Bihar', 'Jharkhand',
                   'Rajasthan']
#, 'Uttar Pradesh', 'Maharashtra'] 

# date range to limit form submissions between
# (in march 2018, every GMP form linked to an AGMP form)
start_date = pd.Timestamp('03-20-2017')
end_date = pd.Timestamp('03-01-2018') 

## ------------- don't edit below here -----------------------------

# start logging
gf.start_logging(output_dir)
gmp_date_cols = ['received_on']
os.chdir(output_dir)

# Pull in all GMP forms between 3/20/17 and 3/1/18
# enough data here that getting in two different downloads, combining here
gmp_df = pd.DataFrame()
gmp_df = gmp_df.fillna('')
gmp_folders = os.listdir(gmp_dir)
for folder in gmp_folders:
    logging.info('Going through data for: %s' % folder)
    gmp_input = gf.csv_files_to_df(os.path.join(gmp_dir, folder), data_regex, date_cols = gmp_date_cols)
    
    # drop data that is out of data range of interest
    gmp_input['received_on'] = pd.to_datetime(gmp_input['received_on'])
    logging.info('Trimming data to before %s' % (end_date))
    gmp_start_size = gmp_input.shape[0]
    #trimmed_gmp_input = gmp_input[(gmp_input['received_on'] >= start_date) & (gmp_input['received_on'] <= end_date)]
    trimmed_gmp_input = gmp_input[(gmp_input['received_on'] <= end_date)]
    logging.info('Trimmed by %i' % (gmp_start_size - trimmed_gmp_input.shape[0]))
    gmp_df = gmp_df.append(trimmed_gmp_input)
    logging.info('gmp rows: %s' % gmp_df.shape[0])

# Limit to real locations
logging.info('raw gmp forms: %i' % gmp_df.shape[0])
gmp_df = gf.add_locations_by_username(gmp_df)
logging.info('raw gmp forms after add locations: %i' % gmp_df.shape[0])
gmp_df = gmp_df.loc[(gmp_df['state_name'].isin(real_state_list))]
logging.info('Num GMP forms in real locations: %i' % gmp_df.shape[0])
logging.info('------------------------------------------')

# See how many of these forms were yellow/red. Any yellow/red would have been linked to an AGMP form
logging.info('GMP form zscore_grading_wfa counts:')
logging.info(gmp_df['form.zscore_grading_wfa'].value_counts(dropna=False))
gmp_linked = gmp_df[(gmp_df['form.zscore_grading_wfa'] == 'yellow') | (gmp_df['form.zscore_grading_wfa'] == 'red')]
gmp_link_count = gmp_linked.shape[0]
logging.info('GMP forms that should have been linked: %i' % gmp_link_count)

# Pull in all AGMP forms between 3/20/17 and 3/1/18
agmp_df = pd.DataFrame()
agmp_df = agmp_df.fillna('')

logging.info('Going through data for: %s' % agmp_dir)
agmp_input = gf.csv_files_to_df(agmp_dir, data_regex, date_cols = gmp_date_cols)

# drop data that is out of data range of interest
agmp_input['received_on'] = pd.to_datetime(agmp_input['received_on'])
start_size = agmp_input.shape[0]
logging.info('Trimming data to before %s' % (end_date))
#agmp_df = agmp_input[(agmp_input['received_on'] >= start_date) & (agmp_input['received_on'] <= end_date)]
agmp_df = agmp_input[(agmp_input['received_on'] <= end_date)]
logging.info('Trimmed by %i' % (start_size - agmp_df.shape[0]))

# Limit to real locations
logging.info('raw agmp forms: %i' % agmp_df.shape[0])
agmp_df = gf.add_locations_by_username(agmp_df)
logging.info('raw agmp forms after add locations: %i' % agmp_df.shape[0])
agmp_df = agmp_df.loc[(agmp_df['state_name'].isin(real_state_list))]
logging.info('Num AGMP forms in real locations: %i' % agmp_df.shape[0])

# Look at this number in comparison to the numbers above - gives a _rough_ idea of non-linked AGMP submissions
logging.info('Approx number of AGMP forms filled out without linking: %i' % (gmp_link_count - agmp_df.shape[0]))

# by state?
gmp_state_df = gmp_linked.groupby(['state_name'])['received_on'].count()
gmp_state_df = gmp_state_df.rename('gmp_linked')
agmp_state_df = agmp_df.groupby(['state_name'])['received_on'].count()
agmp_state_df = agmp_state_df.rename('all_agmp')
state_df = pd.DataFrame()
state_df = pd.concat([gmp_state_df, agmp_state_df], axis=1)
state_df.loc['Total'] = state_df.sum(axis=0)
state_df['diff'] = state_df['all_agmp'] - state_df['gmp_linked']
state_df['%_not_linked'] = state_df['diff'] * 100. / state_df['all_agmp']

# lets add on % agmp that are none for kicks
agmp_none = agmp_df.groupby(['state_name'])['form.measure_identify | none'].count()
agmp_none = agmp_none.rename('measure_identify_none')
state_df = pd.concat([state_df, agmp_none], axis=1)
state_df['pct AGMP none'] = state_df['measure_identify_none'] * 100. / state_df['all_agmp']

logging.info(state_df)
logging.info('AGMP Measure Identify multi-select by state:')
logging.info(agmp_df.groupby(['state_name'])['form.measure_identify | none','form.measure_identify | height','form.measure_identify | muac'].count())
