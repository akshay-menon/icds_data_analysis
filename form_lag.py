# -*- coding: utf-8 -*-
"""
Created on Wed Dec 06 07:33:50 2017

@author: theism
"""
import gen_func as gf
import form_func as ff
import pandas as pd
import numpy as np
from datetime import date
import re
import os
import logging
from matplotlib import pyplot as plt

# ----------------  USER EDITS -------------------------------
# Specify data location, regex to use on files, date columns that are dates
# Future - consider a GUI to do this input better

target_dir = 'C:\\Users\\theism\\Documents\\Dimagi\\Data\\Form Submissions'
data_regex = re.compile(r'icds.\d+\.\d+.\d\d\d\d.csv')

# output_dir = 'C:\\Users\\theism\\Documents\\Dimagi\\Results\\Form Submissions\\testing'
output_dir = 'C:\\Users\\theism\\Documents\\Dimagi\\Results\\Form Lag'
update_output_files = True

# note that UCR code not implemented until very late oct 2017
start_date = pd.Timestamp('11-01-2017')
#end_date = pd.Timestamp('12-07-2017') 
end_date = pd.Timestamp(date.today()) 

# set states to plot
real_state_list = ['Madhya Pradesh', 'Chhattisgarh', 'Andhra Pradesh', 'Bihar',
                   'Jharkhand', 'Rajasthan', 'Uttar Pradesh', 'Maharashtra']

# set buckets for time lag
bins = [-1000, -0.25, 1, 6, 12, 24, 48, 72, 1000]
bin_names = ['<0', '0-1 hr', '1-6 hrs', '6-12 hrs', '12-24 hrs', '24-48 hrs', '48-72 hrs', '72+ hrs']

# ------------- don't edit below here -----------------------------
location_file_dir = r'C:\Users\theism\Documents\Dimagi\Data\static-awc_location.csv'

# start logging and initialize dfs
gf.start_logging(output_dir)
form_time_df = pd.DataFrame()
form_time_df = form_time_df.fillna('')
summary_index = ['mean', 'median', 'num_nan', 'total_rows', 'time_data_rows']
summary_df = pd.DataFrame(columns=summary_index)
summary_df = summary_df.fillna('')
bucket_df = pd.DataFrame(columns=bin_names)
bucket_df = bucket_df.fillna('')
state_summ_df = pd.DataFrame()
state_sum_df = state_summ_df.fillna('')
all_data = pd.DataFrame()
all_data = all_data.fillna('')

# for stats for each district
dist_df = pd.read_csv(location_file_dir, usecols=['district_name', 'state_name'])
dist_df = dist_df.loc[(dist_df['state_name'].isin(real_state_list))]
dist_df = dist_df.drop_duplicates('district_name').reset_index().set_index('district_name').drop('index', axis=1)

# specify what data cols to get
date_cols = ['form_date', 'form_time', 'received_on']
col_names = date_cols + ['awc_id']
location_columns = ['doc_id', 'state_name', 'district_name']
file_list = ff.file_subset_by_date(start_date, end_date, target_dir, data_regex)

# iterate through files to build single dataframe
for data_file in file_list:
    file_date = data_file[5:-4]
    logging.info('Going through data for: %s' % data_file)
    # get data into input file
    input_df = pd.read_csv(os.path.join(target_dir, data_file), 
                           usecols=col_names, parse_dates=date_cols,
                           infer_datetime_format=True)
    
    # add locations to data and set indices
    input_df = gf.add_locations(input_df, 'awc_id', location_columns)
    input_df = input_df.loc[(input_df['state_name'].isin(real_state_list))]
        
    # add a few columns we care about - convert to days decimal point
    input_df['time_lag'] = input_df['received_on'] - input_df['form_time']
    input_df['time_lag_hrs'] = input_df['time_lag'] / np.timedelta64(1, 'h')
    
    # add buckets
    input_df['time_bucket'] = pd.cut(input_df['time_lag_hrs'], bins, labels=bin_names)
    
    # create output dataframes we care about
    # by states
    state_df = input_df.groupby(['form_date', 'state_name']).mean()
    state_df['median'] = input_df.groupby(['form_date', 'state_name']).median()
    state_buckets = input_df.groupby(['state_name'])['time_bucket'].value_counts().unstack().fillna(0)
    state_buckets['total'] = state_buckets.sum(axis=1)
    state_df = state_df.join(state_buckets)
    state_summ_df = state_summ_df.append(state_df)
        
    # for all data
    this_date = input_df['form_date'].iloc[0]
    this_dates_mean = input_df['time_lag_hrs'].mean()
    this_dates_median = input_df['time_lag_hrs'].median()
    total_rows = input_df['awc_id'].count()
    time_data_rows = input_df['time_bucket'].count()
    na_values = total_rows - time_data_rows
    summary_data = [this_dates_mean, this_dates_median, na_values, total_rows, time_data_rows]
    summary_df = summary_df.append(pd.Series(summary_data,
                                             name=this_date,
                                             index=summary_index))
    
    # output stats thus far
    logging.info('%s: mean=%.1f median=%.1f totalrows=%.0f timedatarows=%0.1f numna=%.0f' %
                 (this_date, this_dates_mean, this_dates_median, total_rows, time_data_rows, na_values))

    # create table of bucket values
    bucket_vals = pd.Series(input_df['time_bucket'].value_counts(), name=this_date)
    bucket_df = bucket_df.append(bucket_vals)
    
    # get district info too
    dist_df[file_date] = input_df.groupby('district_name').median()
    
    all_data = all_data.append(input_df)
    
# manipulate bucket data
os.chdir(output_dir)
bucket_df['total'] = bucket_df.sum(axis=1)
per_col_names = []
for mybin in bin_names:
    new = '% ' + mybin
    if mybin not in bucket_df.columns:
        bucket_df[mybin] = 0
    bucket_df[new] = (bucket_df[mybin] / bucket_df['total'] * 100)
    if mybin not in state_summ_df.columns:
        state_summ_df[mybin] = 0
    state_summ_df[new] = (state_summ_df[mybin] / state_summ_df['total'] * 100)
    per_col_names.append(new)

bucket_df.plot(kind='line', stacked=False, figsize=[16,6], y=per_col_names,
               title='Time Lag Bucket Percentage by Date')
if update_output_files:
    plt.savefig('time_lag_bucket_percentage.png')

logging.info('Mean of all data: %.1f' % summary_df['mean'].mean())
logging.info('Median of all data: %.1f' % summary_df['median'].mean())
logging.info('Average percentages for buckets across all dates:')
logging.info(bucket_df[per_col_names].mean().round(1))

summary_df.plot(kind='line', figsize=[16,6], y=['mean', 'median'],
               title='Lag from Form End Time to Server Received Time in Hrs')
if update_output_files:
    plt.savefig('time_lag_time_series.png')
    
output_df = summary_df.join(bucket_df)

# get list of aww's whose phone date may be suspect from last iteration
awc_bad_time_list = input_df[input_df['time_bucket'] == '<0'].groupby(['awc_id']).size().to_frame(name='count').reset_index()
awc_bad_time_list = gf.add_locations(awc_bad_time_list, 'awc_id', ['doc_id', 'awc_name', 'district_name', 'state_name'])

# summarize for each state
state_summ_avrg = state_summ_df.reset_index()
state_summ_avrg = state_summ_avrg.groupby(['state_name']).mean()

for state in real_state_list:
    all_data[all_data['state_name'] == state].boxplot(column='time_lag_hrs', by='form_date', rot=45, figsize=(16,12))
    plt.title('Boxplot grouped by form_data for %s' % state)
all_data.boxplot(column='time_lag_hrs', by='form_date', rot=45, figsize=(16,12))

if update_output_files:
    dist_df.to_csv('district_median_lag.csv')
    output_df.to_csv('lag_by_day_summary.csv')
    state_summ_df.to_csv('lag_by_day_by_state.csv')
    awc_bad_time_list.to_csv('awws_w_bad_phone_times.csv')
    state_summ_avrg.to_csv('state_summary_data.csv')
