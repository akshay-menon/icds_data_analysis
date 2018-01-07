# -*- coding: utf-8 -*-
"""
Analyzes user activity levels based on form submissions.  Replicates Neal's R
work.

Created on Mon Oct 16 14:20:47 2017

@author: theism
"""
import gen_func as gf
import form_func as ff
import pandas as pd
import os
import logging
import re
import time
from datetime import date
import numpy as np
from matplotlib import pyplot as plt
import seaborn as sns

# ----------------  USER EDITS -------------------------------
# Specify data location, regex to use on files, date columns that are dates
target_dir = 'C:\\Users\\theism\\Documents\\Dimagi\\Data\\Form Submissions'
data_regex = re.compile(r'icds.\d+\.\d+.\d\d\d\d.csv')

output_dir = 'C:\\Users\\theism\\Documents\\Dimagi\\Results\\User Activity'

# if only want to test with a subset of dates:
start_date = pd.Timestamp('03-20-2017')
#end_date = pd.Timestamp('04-10-2017')
end_date = pd.Timestamp(date.today())  

# will download new files if you don't have them in your target_dir already
# NOTE - if already have the output daily_forms_db, don't need the old data
download_new = True
download_start = pd.Timestamp('03-20-2017')
download_stop = pd.Timestamp(date.today())

# will remove the last refresh_days of files and download them again, in case
# all forms haven't been submitted for those days yet
refresh_recent = False
refresh_days = 10

# allows all data to be recalculated
recalc = False

# csv file where you have a user/pass to log into commcare to dwnld ucr
credential_path = r'C:\Users\theism\Documents\Dimagi\Admin\user_info.csv'

# set states to plot
real_state_list = ['Madhya Pradesh', 'Chhattisgarh',
                   'Andhra Pradesh', 'Bihar', 'Jharkhand',
                   'Rajasthan', 'Uttar Pradesh', 'Maharashtra']  

# ------------- don't edit below here -----------------------------
def assign_inactive(x):
    '''Return series of cumulative sums of inactive users'''
    # is a pandas series - need to figure out how/if to assign inactive_groups
    return x.groupby(x.ne(x.shift()).cumsum()).cumcount() + 1


def inactive_group(value):
    '''Returns string that represents inactive user groupings'''
    if 'inactive_' in value:
        x = int(value[9:])
        if x <= 3:
            return 'inactive_1-3'
        elif x <= 7:
            return 'inactive_4-7'
        elif x <= 14:
            return 'inactive_8-14'
        elif x <= 28:
            return 'inactive_15-28'
        elif x <= 90:
            return 'inactive_29-90'
        else:
            return 'inactive_90+'
    else:
        return value

# start logging
gf.start_logging(output_dir)

col_names = ['form_date', 'awc_id', 'pse', 'gmp', 'thr', 'add_household',
             'add_person', 'add_pregnancy', 'home_visit', 'due_list_ccs',
             'due_list_child']

# output file to save aggregate data
saved_output = 'aww_daily_forms_db.csv'
saved_file = os.path.join(output_dir, saved_output)

# initialize outputs
if os.path.isfile(saved_file) is False:
    forms_df = pd.DataFrame()
    forms_df = forms_df.fillna(0)
    logging.info('Didnt find existing forms db file.  Creating a new one.')
# save file already exists.  import and make sure index column in expected fmt
else:
    forms_df = pd.read_csv(saved_file, header=0)
    forms_df = forms_df.set_index(['awc_id'])
    # get rid of locations,etc.  add back again once have added new data to recalc
    forms_df = forms_df.drop(['block_name', 'district_name', 'state_name',
                              'last_submission', 'start_date', 'days_inactive',
                              'days_since_start'], axis=1)

# need to download new files if set to True
if download_new is True:
    file_list = ff.download_form_ucr(download_start, download_stop, credential_path,
                                     target_dir, refresh_recent, refresh_days, data_regex)
else:
    file_list = ff.file_subset_by_date(start_date, end_date, target_dir, data_regex)

# if recalc, lets refresh the location data too
if recalc is True:
    gf.refresh_locations()

logging.info(time.strftime('%X %x'))
logging.info('Starting scripts to analyze data...')
new_forms_df = pd.DataFrame()
new_forms_df = new_forms_df.fillna(0)
# run through each file in the target data directory
date_columns = pd.to_datetime(forms_df.columns)
for data_file in file_list:
    logging.info('Seeing if %s is already in output' % data_file)
    file_date = data_file[5:-4]
    # if file data not in file, then need to crunch the numbers and add it
    # pandas magically knows that d.M.Y can match Y-D-M
    if file_date not in date_columns or recalc is True:
        logging.debug('-------------------------------------------')
        logging.debug('Going through data for: %s' % data_file)
        logging.debug('-------------------------------------------')
        logging.debug(time.strftime('%X %x'))

        # get data
        input_df = pd.read_csv(os.path.join(target_dir, data_file),
                               usecols=col_names, parse_dates=['form_date'],
                               infer_datetime_format=True)

        # add column for totals, name of column is date
        submission_date = input_df.loc[0]['form_date'].strftime('%Y-%m-%d')
        input_df = input_df.groupby('awc_id').sum()
        total_forms = input_df.sum(axis=1)
        user_df = total_forms.to_frame(name=submission_date)

        # add new column with date
        new_forms_df = pd.concat([new_forms_df, user_df], axis=1)
        logging.info('Added new date column %s' % file_date)
    else:
        logging.info('Found %s in file already' % file_date)

# make sure columns are in ascending order. dont rely on filenames
logging.info(time.strftime('%X %x'))
forms_df = pd.concat([forms_df, new_forms_df], axis=1)
date_cols = sorted(list(forms_df.columns.values))
forms_df = forms_df[date_cols]
# new_date_cols = sorted(list(new_forms_df.columns.values))
# new_forms_df = new_forms_df[new_date_cols]

# set NaN to 0
forms_df = forms_df.fillna(0)

# start_date: column index of max on each row, which gives the start date
# last_submission:  reverse the order of columns and find the column index of
# max on each row again, which gives the end date
# do in one step so adding start date column doesn't interfere with last sub.
forms_df = forms_df.assign(start_date=forms_df.astype(int).astype(bool).idxmax(1), 
                           last_submission=forms_df.astype(int).astype(bool)[forms_df.columns[::-1]].idxmax(1))

# add locations to data and set indices
forms_df.index.rename('awc_id', True)
location_columns = ['doc_id', 'block_name', 'district_name', 'state_name']
forms_df = gf.add_locations(forms_df, None, location_columns, True)
# get rid of not real states
forms_df = forms_df.loc[(forms_df['state_name'].isin(real_state_list))]
# reorder columns
col_order = location_columns[1:] + ['start_date', 'last_submission'] + date_cols
forms_df = forms_df[col_order]

#-----------------  daily form submissions ----------------------------
# days_inactive: today minus last_submission date
last_date = pd.to_datetime(date_cols[-1])
forms_df = forms_df.assign(days_inactive= lambda x: ((last_date - pd.to_datetime(x.last_submission)) / np.timedelta64(1, 'D')))
forms_df = forms_df.assign(days_since_start= lambda x: ((last_date - pd.to_datetime(x.start_date)) / np.timedelta64(1, 'D')) + 1)

# reorder columns
column_order = (location_columns[1:] + \
                ['start_date', 'last_submission', 'days_inactive', 'days_since_start'] + \
                date_cols)
forms_df = forms_df[column_order]

daily_forms_output = os.path.join(output_dir, saved_output)
forms_df.to_csv(daily_forms_output, date_format='%m-%d-%Y')
logging.info('output file saved to %s' % daily_forms_output)

# ------------------ tallies --------------------------
tallies_df = forms_df.filter(['block_name',
                              'district_name', 'state_name',
                              'last_submission', 'start_date',
                              'days_inactive', 'days_since_start'])
tallies_df = gf.add_locations(tallies_df, None, ['doc_id', 'awc_name'])
tallies_df = tallies_df.reset_index().set_index('awc_name').drop('awc_id', axis=1)
tallies_output = os.path.join(output_dir, 'tallies_' + last_date.strftime('%Y-%m-%d') + '.csv')
tallies_df.to_csv(tallies_output, date_format='%m-%d-%Y')
logging.info('output file saved to %s' % tallies_output)

#-----------------  activity status ----------------------------
# TODO - see if able to only do for new_forms_df - tricky since cumsum looks through each row from start
'''
# decide if need to append new rows to existing file, or write new one
if os.path.isfile(activity_output) is True:
    logging.info('Appending to activity status file...')
    old_activity_df = pd.read_csv(activity_output, header=0)
    activity_df = pd.concat([old_activity_df, new_forms_df], axis=1)
else:
    activity_df = forms_df.copy()

# only run calcs for new forms since time consuming
if not new_forms_df.empty:
''' 
activity_output = os.path.join(output_dir, 'activity_status.csv')
activity_df = forms_df.copy()

# assign 'active' if val > 0
logging.info('assigning active users...')
activity_df[date_cols] = activity_df[date_cols].mask(activity_df[date_cols] > 0, "active")

# assign 'start' if col_date == start_date
# https://stackoverflow.com/questions/47858969/pandas-apply-condition-to-column-value-based-on-another-column
logging.info('assigning start dates...')
non_date_cols = len(np.setdiff1d(forms_df.columns.tolist(), date_cols))
temp_forms_np = activity_df.values[:, non_date_cols:]
temp_forms_np[activity_df['start_date'].values[:, None] == activity_df.columns[non_date_cols:].values] = "start"

# assign 'pre' if start_date > col_date
temp_forms_np[activity_df['start_date'].values[:, None] > activity_df.columns[non_date_cols:].values] = "pre"
activity_df.iloc[:, non_date_cols:] = temp_forms_np

# count cumsum of zeros from left to right
# needed lots of help with this one:
# https://stackoverflow.com/questions/47862796/pandas-use-cumsum-over-columns-but-reset-count/47863123#47863123
# TODO - this is the slow part.  not sure how to speed up unless only do for new columns, but uses cumsum...
logging.info('assigning inactive users...')
i = activity_df[date_cols].apply(pd.to_numeric, errors='coerce')
j = 'inactive_' + i.apply(assign_inactive, axis=1).astype(str)
activity_df[date_cols] = np.where(i.ne(0), activity_df[date_cols].values, j)
    
# group the inactive dates
logging.info('assigning inactive groups...')
activity_df[date_cols] = activity_df[date_cols].apply(np.vectorize(inactive_group))

# phew - now create output.  thank goodness for stackexchange
activity_df.to_csv(activity_output, date_format='%m-%d-%Y')
logging.info('output file saved to %s' % activity_output)

# ------------------------ dropoff X days -------------------------
dropoff_df = tallies_df.copy()
#dropoff_10['total_users'] = dropoff_df['block_name'].value_counts()
dropoff_df['started >= 10 days ago'] = dropoff_df['days_since_start'] >= 10
dropoff_df['inactive >= 10 days'] = dropoff_df['days_inactive'] >= 10
dropoff_df['total_users'] = 1
dropoff_10 = dropoff_df.groupby(['block_name'])['total_users', 'started >= 10 days ago', 'inactive >= 10 days'].sum().astype(int)
dropoff_10['percent_inactive'] = dropoff_10['inactive >= 10 days'] / dropoff_10['started >= 10 days ago']  * 100
dropoff_10['percent_inactive'] = dropoff_10['percent_inactive'].fillna(0)
# add locations back again
dropoff_loc_cols = ['block_name', 'district_name', 'state_name']
dropoff_10 = gf.add_locations(dropoff_10, None, dropoff_loc_cols)

dropoff_output = os.path.join(output_dir, 'dropoff_10_by_block_' + last_date.strftime('%Y-%m-%d') + '.csv')
dropoff_10.to_csv(dropoff_output, date_format='%m-%d-%Y')
logging.info('output file saved to %s' % dropoff_output)

# ---------------- tables ---------------------
# note - row_order values need to match above groupings exactly...
row_order = ['pre', 'start', 'active', 'inactive_1-3', 'inactive_4-7',
             'inactive_8-14', 'inactive_15-28', 'inactive_29-90', 'inactive_90+']

# group all the status_types
# https://stackoverflow.com/questions/46863602/find-value-counts-within-a-pandas-dataframe-of-strings/46864423#46864423
summary_df = activity_df[date_cols].apply(pd.Series.value_counts).fillna(0)
summary_df = summary_df.reindex(row_order)
# drop the 'pre' row for plotting
summary_df = summary_df.drop('pre')

# also create a state df for status_types
state_summary_df = pd.DataFrame()
state_summary_pct_df = pd.DataFrame()
for state in real_state_list:
    if state in activity_df['state_name'].tolist():
        state_activity_df = activity_df[activity_df['state_name'] == state]
        temp_state_summary_df = state_activity_df[date_cols].apply(pd.Series.value_counts).fillna(0)
        temp_state_summary_df = temp_state_summary_df.reindex(row_order)
        temp_state_summary_df = temp_state_summary_df.reset_index()
        temp_state_summary_df['state_name'] = state
        temp_state_summary_df.rename(columns={'index':'status_type'}, inplace=True)
        temp_state_summary_df = temp_state_summary_df.set_index(['state_name', 'status_type'])
        temp_state_summary_df = temp_state_summary_df.drop('pre', level=1)
        temp_pct = temp_state_summary_df / temp_state_summary_df.sum() * 100
        state_summary_df = state_summary_df.append(temp_state_summary_df)
        state_summary_pct_df = state_summary_pct_df.append(temp_pct)
        

# ---------------- plots  for summary data ---------------------

os.chdir(output_dir)

# transpose and plot (plotting is easier for transposed rows)
summary_df = summary_df
summary_dfT = summary_df.transpose()
summary_dfT.plot(kind='area',
                figsize=[16, 6],
                stacked=True,
                title='User Activity Levels by Day')
plt.legend(bbox_to_anchor=(1, 1), loc=2)
plt.savefig('user_activity_levels_' + last_date.strftime('%Y-%m-%d') + '.png')

# get % buckets
summary_pct_df = summary_df / summary_df.sum() * 100
summary_pct_dfT = summary_pct_df.transpose()

# plot stacked lines by pct of form submissions by day
summary_pct_dfT.plot(kind='area',
                    figsize=[16, 6],
                    stacked=True,
                    title='User Activity by Percent by Day')
plt.ylim([0, 100])
plt.legend(bbox_to_anchor=(1, 1), loc=2)
plt.savefig('activity_pct_' + last_date.strftime('%Y-%m-%d') + '.png')

summary_out = summary_df.append(summary_pct_df)
summary_file = os.path.join(output_dir, 'summary.csv')
summary_out.to_csv(summary_file, date_format='%m-%d-%Y')
logging.info('output file saved to %s' % summary_file)

logging.info('Most recent activity:')
logging.info(summary_out.iloc[:,-1])


# --------------------- state stuff-------------------------
f, axarr = plt.subplots(3, 2, figsize=[16, 12])
count = 0
for row in range(0, 3):
    for col in range(0, 2):
        if count < len(real_state_list):
            state_summary_df.loc[real_state_list[count],:].transpose().plot(kind='area',ax=axarr[row, col], stacked=True, legend=False)
            axarr[row, col].set_title(real_state_list[count])
            count += 1
f.suptitle('User Activity Levels by Day by State')
axarr[0, 1].legend(bbox_to_anchor=(1, 1), loc=2)
x=axarr[0, 0].set_xticklabels(labels='', visible=False)
x=axarr[0, 1].set_xticklabels(labels='', visible=False)
x=axarr[1, 0].set_xticklabels(labels='', visible=False)
x=axarr[1, 1].set_xticklabels(labels='', visible=False)
y=axarr[0, 0].set_xlabel(xlabel='', labelpad=None)
y=axarr[0, 1].set_xlabel(xlabel='', labelpad=None)
y=axarr[1, 0].set_xlabel(xlabel='', labelpad=None)
y=axarr[1, 1].set_xlabel(xlabel='', labelpad=None)
plt.savefig('user_activity_levels_state_' + last_date.strftime('%Y-%m-%d') + '.png')

f, axarr = plt.subplots(3, 2, figsize=[16, 12])
count = 0
for row in range(0, 3):
    for col in range(0, 2):
        if count < len(real_state_list):
            state_summary_pct_df.loc[real_state_list[count],:].transpose().plot(kind='area', stacked=True,ax=axarr[row, col], legend=False)
            axarr[row, col].set_title(real_state_list[count])
            count += 1
f.suptitle('User Activity by Percent by Day by State')
axarr[0, 1].legend(bbox_to_anchor=(1, 1), loc=2)
x=axarr[0, 0].set_xticklabels(labels='', visible=False)
x=axarr[0, 1].set_xticklabels(labels='', visible=False)
x=axarr[1, 0].set_xticklabels(labels='', visible=False)
x=axarr[1, 1].set_xticklabels(labels='', visible=False)
y=axarr[0, 0].set_xlabel(xlabel='', labelpad=None)
y=axarr[0, 1].set_xlabel(xlabel='', labelpad=None)
y=axarr[1, 0].set_xlabel(xlabel='', labelpad=None)
y=axarr[1, 1].set_xlabel(xlabel='', labelpad=None)
plt.savefig('activity_pct_state_' + last_date.strftime('%Y-%m-%d') + '.png')

state_summary_df.to_csv(os.path.join(output_dir, 'state_summary.csv'), date_format='%m-%d-%Y')
state_summary_pct_df.to_csv(os.path.join(output_dir, 'state_pct_summary.csv'), date_format='%m-%d-%Y')


logging.info(time.strftime('%X %x'))