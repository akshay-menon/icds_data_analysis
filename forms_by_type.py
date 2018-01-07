# -*- coding: utf-8 -*-
"""
Analyzes form submissions by type of form.  User can specify whether to
download new UCR data, whether to delete & refresh recent data to get all new
submissions that have trickled in.  Saves two output files, one with state
information, one without.  Makes some plots too.

Created on Sun Sep 24 19:01:00 2017

@author: theism
"""
import gen_func as gf
import form_func as ff
import os
import logging
import re
import pandas as pd
import time
from datetime import date, datetime
import numpy as np
import calendar
from matplotlib import pyplot as plt
import matplotlib as mpl

# ----------------  USER EDITS -------------------------------
# Specify data location, regex to use on files, date columns that are dates
# Future - consider a GUI to do this input better

target_dir = 'C:\\Users\\theism\\Documents\\Dimagi\\Data\\Form Submissions'
data_regex = re.compile(r'icds.\d+\.\d+.\d\d\d\d.csv')

# output_dir = 'C:\\Users\\theism\\Documents\\Dimagi\\Results\\Form Submissions\\testing'
output_dir = 'C:\\Users\\theism\\Documents\\Dimagi\\Results\\Form Submissions'

# will download new files if you don't have them in your target_dir already
# NOTE - if already have the output daily_forms_db, don't need the old data
download_new = True
download_start = pd.Timestamp('03-20-2017')
#download_stop = pd.Timestamp(date.today()) 
download_stop = pd.Timestamp('12-09-2017') 

# will remove the last refresh_days of files and download them again, in case
# all forms haven't been submitted for those days yet
# NOTE - requires download_new = True to operate
refresh_recent = True
refresh_days = 10

# allows all data to be recalculated
recalc = False

# csv file where you have a user/pass to log into commcare to dwnld ucr
credential_path = r'C:\Users\theism\Documents\Dimagi\Admin\user_info.csv'

# if only want a subset of data plotted
trim_dates = False
trim_start = pd.Timestamp('9-01-2017')
# for today, put: date.today()
trim_stop = pd.Timestamp('11-01-2017')

# set states to plot
real_state_list = ['Madhya Pradesh', 'Chhattisgarh',
                   'Andhra Pradesh', 'Bihar', 'Jharkhand',
                   'Rajasthan']
#, 'Uttar Pradesh', 'Maharashtra']  

# ------------- don't edit below here -----------------------------

# start logging
gf.start_logging(output_dir)
start_time = datetime.now()

date_cols = ['form_date']
col_names = ['form_date', 'awc_id', 'pse', 'gmp', 'thr', 'add_household',
             'add_person', 'add_pregnancy', 'home_visit', 'bp_tri1', 'bp_tri2',
             'bp_tri3', 'delivery', 'pnc', 'ebf', 'cf', 'due_list_ccs',
             'due_list_child']

# output file to save aggregate data
saved_output = 'daily_forms_db.csv'
saved_file = os.path.join(output_dir, saved_output)

# initialize outputs
if os.path.isfile(saved_file) is False:
    forms_df = pd.DataFrame()
    forms_df = forms_df.fillna('')
    logging.info('Didnt find existing forms db file.  Creating a new one.')
# save file already exists.  import and make sure index column in expected fmt
else:
    forms_df = pd.read_csv(saved_file, header=0, parse_dates=date_cols,
                           infer_datetime_format=True)
    forms_df = forms_df.set_index(['form_date', 'state_name'])

# need to download new files if set to True
if download_new is True:
    file_list = ff.download_form_ucr(download_start, download_stop, credential_path,
                                     target_dir, refresh_recent, refresh_days, data_regex)
else:
    file_list = gf.data_file_list(target_dir, data_regex)

# if recalc, lets refresh the location data too
if recalc is True:
    gf.refresh_locations()

logging.info('Starting scripts to analyze data...')

location_columns = ['doc_id', 'state_name']
for data_file in file_list:
    logging.info('Seeing if %s is already in output' % data_file)
    file_date = data_file[5:-4]
    # pandas magically knows that d.M.Y can match Y-D-M
    if file_date not in forms_df.index or recalc is True:
        logging.info('Going through data for: %s' % data_file)
        logging.debug(time.strftime('%X %x'))

        # get data
        input_df = pd.read_csv(os.path.join(target_dir, data_file), 
                               usecols=col_names, parse_dates=date_cols,
                                infer_datetime_format=True)
        
        # add locations to data and set indices
        input_df = gf.add_locations(input_df, 'awc_id', location_columns)
        input_df = input_df.loc[(input_df['state_name'].isin(real_state_list))]
        input_df = input_df.set_index(['form_date', 'state_name'])

        # minimize data to just the counts
        min_in_df = input_df.groupby(['form_date', 'state_name']).sum()

        # put data into the categories we care about
        min_in_df['due_list'] = min_in_df['due_list_ccs'] + \
                                min_in_df['due_list_child']
        min_in_df['hh_mng'] = min_in_df['add_household'] + \
                              min_in_df['add_person'] + \
                              min_in_df['add_pregnancy']
        min_in_df['bp'] = min_in_df['bp_tri1'] + \
                          min_in_df['bp_tri2'] + \
                          min_in_df['bp_tri3']

        min_in_df['total'] = min_in_df['pse'] + \
                             min_in_df['gmp'] + \
                             min_in_df['thr'] + \
                             min_in_df['home_visit'] + \
                             min_in_df['due_list'] + \
                             min_in_df['hh_mng']

        # add percentage columns
        min_in_df = ff.divisor('pct', 'total', min_in_df, multiplier=100)

        # add day of week column - interesting data here
        min_in_df['weekday'] = pd.to_datetime(min_in_df.index[0][0]).weekday_name

        # add new row with date
        forms_df = forms_df.append(min_in_df)
        logging.debug('Appended new data to the file.')
    else:
        logging.debug('Found %s in file already' % file_date)

print('elapsed time: %s' % (datetime.now() - start_time))
# save file for later use - with state information
output_file1 = os.path.join(output_dir, saved_output)
forms_df.to_csv(output_file1, date_format='%m-%d-%Y')
logging.info('output file saved to %s' % output_file1)

# also save a flat file with no state information
forms_df_flat = forms_df.groupby(['form_date', 'weekday']).sum().reset_index().set_index(['form_date'])
forms_df_flat = ff.divisor('pct', 'total', forms_df_flat, multiplier=100)
output_file2 = os.path.join(output_dir, 'daily_forms_db_flat.csv')
forms_df_flat.to_csv(output_file2, date_format='%m-%d-%Y')
logging.info('output file saved to %s' % output_file2)

logging.info('numbers for the total column for each day')
logging.info(forms_df_flat['total'].describe())
totals = forms_df_flat.sum()
logging.info('pse avrg: %0.1f' % (float(totals['pse']) / float(totals['total']) * 100))
logging.info('gmp avrg: %0.1f' % (float(totals['gmp']) / float(totals['total']) * 100))
logging.info('thr avrg: %0.1f' % (float(totals['thr']) / float(totals['total']) * 100))
logging.info('home_visit avrg: %0.1f' % (float(totals['home_visit']) / float(totals['total']) * 100))
logging.info('due_list avrg: %0.1f' % (float(totals['due_list']) / float(totals['total']) * 100))
logging.info('hh_mng avrg: %0.1f' % (float(totals['hh_mng']) / float(totals['total']) * 100))

# ------------------------- PLOTTING -------------------------------------

# categories to use later for slicing and dicing
plot_cats = {'form_pct': ['pse_pct', 'gmp_pct', 'thr_pct', 'home_visit_pct', 'due_list_pct', 'hh_mng_pct'],
             'form_nums': ['pse', 'gmp', 'thr', 'home_visit', 'due_list', 'hh_mng'],
             'form_detailed': ['add_household', 'add_person', 'add_pregnancy', 'bp', 'bp_tri1', 'bp_tri2', 'bp_tri3', 'delivery', 'pnc', 'ebf', 'cf', 'due_list_ccs', 'due_list_child'],
             'home_visits': ['bp', 'delivery', 'pnc', 'ebf', 'cf'],
             'bp_visits': ['bp_tri1', 'bp_tri2', 'bp_tri3'],
             'due_list_visits': ['due_list_ccs', 'due_list_child']}

# if only want to see certain time period
if trim_dates is True:
    forms_df = forms_df.reset_index().set_index('form_date').loc[trim_start:trim_stop]
    forms_df = forms_df.reset_index().set_index(['form_date', 'state_name'])
    forms_df_flat = forms_df_flat.loc[trim_start:trim_stop, :]
    start = trim_start
    stop = trim_stop
elif download_new is True:
    start = download_start
    stop = download_stop
else:
    start = forms_df_flat.index[0]
    stop = forms_df_flat.index[-1]

# set basic plot parameters and create data for plotting
os.chdir(os.path.join(output_dir, 'plots'))
state_forms_df = forms_df.reset_index().set_index('form_date')

# ------------------------- FORM SUBMISSIONS BY DAY -------------------------
# plot stacked lines by pct
forms_df_flat.plot(kind='area',
                   figsize=[16, 6],
                   stacked=True,
                   y=plot_cats['form_pct'],
                   title='Percent of Form Submissions by Day')
plt.ylim([0, 100])
plt.legend(bbox_to_anchor=(1, 1), loc=2)
plt.savefig('forms_areaplot_' + date.today().strftime('%Y-%m-%d') + '.png')

# plot lines for number of form submissions by day - not stacked
forms_df_flat.plot(kind='line',
                   figsize=[16, 6],
                   y=plot_cats['form_nums'],
                   title='Number of Form Submissions by Day')
plt.savefig('forms_category_' + date.today().strftime('%Y-%m-%d') + '.png')

# form submission pct for all states
ff.plot_all_states(forms_df_flat, state_forms_df, plot_cats['form_pct'],
                   real_state_list, start, stop, 'area', True, 'Form Submission Pct')
plt.savefig('forms_areaplot_states_' + date.today().strftime('%Y-%m-%d') + '.png')

# form submission pct for all states
ff.plot_all_states(forms_df_flat, state_forms_df, plot_cats['form_nums'],
                   real_state_list, start, stop, 'line', False, 'Form Submission Numbers')
plt.savefig('forms_category_states_' + date.today().strftime('%Y-%m-%d') + '.png')

ff.plot_all_states(forms_df_flat, state_forms_df, plot_cats['home_visits'],
                  real_state_list, start, stop, 'area', True, 'Home Visit Forms')
plt.savefig('home_visit_forms_states_' + date.today().strftime('%Y-%m-%d') + '.png')

ff.plot_all_states(forms_df_flat, state_forms_df, plot_cats['bp_visits'],
                   real_state_list, start, stop, 'area', True, 'BP Forms')
plt.savefig('bp_forms_states_' + date.today().strftime('%Y-%m-%d') + '.png')

ff.plot_all_states(forms_df_flat, state_forms_df, plot_cats['due_list_visits'],
                   real_state_list, start, stop, 'area', True, 'Due List Forms')
plt.savefig('due_list_forms_states_' + date.today().strftime('%Y-%m-%d') + '.png')



# resampling the 'total' to get moving average of form submissions
# https://stackoverflow.com/questions/15799162/resampling-within-a-pandas-multiindex

# plot resampled form submissions for all data
plt.figure()
forms_df_flat['total'].plot(kind='line', figsize=[16, 6], style=':', title='All Form Submissions - Daily, Weekly Mean, Monthly Mean')
forms_df_flat['total'].resample('1W').mean().plot(kind='line')
forms_df_flat['total'].resample('1M').mean().plot(kind='line')
plt.savefig('avrg_form_submissions_' + date.today().strftime('%Y-%m-%d') + '.png')


# plot resampled for each state
smooth_df = forms_df['total'].reset_index().set_index('form_date')

f, axarr = plt.subplots(3, 2, figsize=[16, 12])
count = 0
for row in range(0, 3):
    for col in range(0, 2):
        if count < len(real_state_list):
            smooth_df[smooth_df['state_name'] == real_state_list[count]]['total'].plot(kind='line', style=':', ax=axarr[row, col])
            smooth_df[smooth_df['state_name'] == real_state_list[count]]['total'].resample('1W').mean().plot(kind='line', ax=axarr[row, col])
            #smooth_df[smooth_df['state_name'] == real_state_list[count]]['total'].resample('2W').mean().plot(kind='line', ax=axarr[row, col])
            smooth_df[smooth_df['state_name'] == real_state_list[count]]['total'].resample('1M').mean().plot(kind='line', ax=axarr[row, col])
            axarr[row, col].set_title(real_state_list[count])
            count += 1
x=axarr[0, 0].set_xticklabels(labels='', visible=False)
x=axarr[0, 1].set_xticklabels(labels='', visible=False)
x=axarr[1, 0].set_xticklabels(labels='', visible=False)
x=axarr[1, 1].set_xticklabels(labels='', visible=False)
y=axarr[0, 0].set_xlabel(xlabel='', labelpad=None)
y=axarr[0, 1].set_xlabel(xlabel='', labelpad=None)
y=axarr[1, 0].set_xlabel(xlabel='', labelpad=None)
y=axarr[1, 1].set_xlabel(xlabel='', labelpad=None)
f.suptitle('Form Submissions by State - Daily, Weekly Mean, Monthly Mean ' 
           + start.strftime('%Y-%m-%d') + ' to ' + stop.strftime('%Y-%m-%d'))
plt.savefig('avrg_form_submissions_states_' + date.today().strftime('%Y-%m-%d') + '.png')

# new plot
plt.figure(figsize=(16, 6))
for state in real_state_list:
    plt.plot(smooth_df[smooth_df['state_name'] == state]['total'].resample('1W').mean(), label=state)
plt.legend()
plt.title('Weekly Mean Form Submissions by State from %s to %s' % (start.strftime('%Y-%m-%d'), stop.strftime('%Y-%m-%d')))
plt.grid(True)
plt.xlim([start, stop])
x=plt.xticks(rotation=0)
plt.savefig('avrg_weekly_form_submissions_' + date.today().strftime('%Y-%m-%d') + '.png')
    
# plot state panels
#for state in real_state_list:
#    ff.state_plot_panel(state_forms_df, state, start, stop, plot_cats)

# plots by day of week
all_states_pivot = pd.pivot_table(forms_df_flat, index=['weekday'], aggfunc=np.average)
all_states_pivot = all_states_pivot.reindex(list(calendar.day_name))
# plot aggregate by day of week
all_states_pivot.plot(kind='bar',
                      stacked=True,
                      figsize=[16,6],
                      y=plot_cats['form_pct'],
                      title='Percent of Form Submissions by Day of Week')               
plt.ylim([0,100])
plt.legend(bbox_to_anchor=(1, 1), loc=2)
plt.xticks(rotation=0)
plt.savefig('pct_weekday_' + date.today().strftime('%Y-%m-%d') + '.png')


all_states_pivot.plot(kind='bar',
                      stacked=True,
                      figsize=[16,6],
                      y=plot_cats['form_nums'],
                      title='Number of Form Submissions by Day of Week')              
plt.legend(bbox_to_anchor=(1, 1), loc=2)
plt.xticks(rotation=0)
plt.savefig('num_weekday_' + date.today().strftime('%Y-%m-%d') + '.png')

# -------------------- submissions per state per worker -----------------------------

# expected submissions per worker.  zero if varies by state/no requirement
expected_nums = {'Andhra Pradesh':
                     {'pse': 25, 'gmp': 65, 'thr': 32,
                      'home_visit': 45, 'due_list': 42.5, 'hh_mng': 1.5},
                 'Bihar': 
                     {'pse': 25, 'gmp': 65, 'thr': 0,
                      'home_visit': 16.25, 'due_list': 143, 'hh_mng': 5},
                 'Chhattisgarh':
                     {'pse': 25, 'gmp': 65, 'thr': 0,
                      'home_visit': 16.25, 'due_list': 143, 'hh_mng': 5},
                 'Jharkhand':
                     {'pse': 25, 'gmp': 22.5, 'thr': 17.5,
                      'home_visit': 62.5, 'due_list': 17.5, 'hh_mng': 5},
                 'Madhya Pradesh':
                     {'pse': 24.5, 'gmp': 75, 'thr': 17.5,
                      'home_visit': 62.5, 'due_list': 17.5, 'hh_mng': 5},
                 'Rajasthan':
                     {'pse': 25, 'gmp': 65, 'thr': 0,
                      'home_visit': 16.25, 'due_list': 143, 'hh_mng': 5},
#                 'Uttar Pradesh':
#                     {'pse': 25, 'gmp': 65, 'thr': 0,
#                      'home_visit': 16.25, 'due_list': 143, 'hh_mng': 5},
#                 'Maharashtra':
#                     {'pse': 25, 'gmp': 65, 'thr': 0,
#                      'home_visit': 16.25, 'due_list': 143, 'hh_mng': 5},
                 'Total':
                     {'pse': 25, 'gmp': 65, 'thr': 0,
                      'home_visit': 16.25, 'due_list': 143, 'hh_mng': 5}
                }


# look at numbers by state and by worker 
all_days_pivot = pd.pivot_table(forms_df, index=['state_name'], aggfunc=np.average).filter(items=(plot_cats['form_nums']))

# create df that is just a column for number of workers in each state
state_index = all_days_pivot.copy()
state_index['num_workers'] = 0
for i in range(0, len(state_index.index.values)):
    state_index['num_workers'][i] = gf.num_by_location('state_name', state_index.index.values[i])
state_index = state_index.filter(items=['num_workers'])
#state_index = pd.Series({'Andhra Pradesh':11008,
#                        'Bihar':12760,
#                        'Chhattisgarh':8500,
#                        'Jharkhand':6500,
#                        'Madhya Pradesh':12840})
#state_index = state_index.to_frame('num_workers')
#state_index.index.name = 'state_name'
state_index.loc['Total'] = state_index.sum()

# add totals row
all_days_pivot.loc['Total'] = all_days_pivot.sum()
# in multindex format of state then form type
all_days_pivot = pd.melt(all_days_pivot.reset_index(),
                         id_vars=['state_name'],
                         var_name='form',
                         value_name='per_state_per_day').set_index(['state_name', 'form'])
all_days_pivot.sort_index(inplace=True)

# get per aww per day form submissions
for state in real_state_list + ['Total']:
    all_days_pivot.loc[(state, slice(None)), 'per_aww_per_day'] = all_days_pivot.loc[(state, slice(None)), 'per_state_per_day'] / state_index.loc[state, 'num_workers']

# add columns for 
all_days_pivot['per_mth'] = all_days_pivot['per_aww_per_day'] * 30.4375

label_list = {}
for state, form_list in expected_nums.iteritems():
    label_list[state] = []
    for form, value in form_list.iteritems():
        all_days_pivot.loc[(state, form), 'diff_per_mth'] = all_days_pivot.loc[(state, form), 'per_mth'] - value
        label = '%s (%.0f)' % (form, value)
        all_days_pivot.loc[(state, form), 'label'] = label
        #label_list[state].append('%s (%.0f)' % (form, value))

# add column for diff pos/neg
all_days_pivot['diff_pos'] = all_days_pivot['diff_per_mth'] > 0

all_days_pivot = all_days_pivot.round(2)
all_days_pivot.to_csv(os.path.join(output_dir, 'form_expectations.csv'))

all_days_reset = all_days_pivot.reset_index().set_index('label')

# make plot
f, axarr = plt.subplots(3, 2, figsize=[16, 12])
mpl.style.use('default')
count = 0
for row in range(0, 3):
    for col in range(0, 2):
        if count < len(real_state_list):
            plot_data = all_days_reset[all_days_reset['state_name'] == real_state_list[count]]
            plot_data.plot(kind='barh', y=['diff_per_mth'],
                           #color=plot_data['diff_pos'].map({True: 'k', False: 'r'}),
                           ax=axarr[row, col], legend=False)
            axarr[row, col].set_title(real_state_list[count])
            #axarr[row, col].set_yticklabels(label_list[real_state_list[count]])
            #axarr[row, col].set_yticklabels(label_list[real_state_list[count]])
            axarr[row, col].set_ylabel(ylabel='', labelpad=None)
            count += 1
f.suptitle('Expected Monthly Form Submissions from %s to %s' % (start.strftime('%Y-%m-%d'), stop.strftime('%Y-%m-%d')))
plt.subplots_adjust(wspace=0.3, hspace=0.3)
plt.savefig('form_sub_vs_exp_' + date.today().strftime('%Y-%m-%d') + '.png')


#-------------------- output some summary tables --------------------------

# mean number of form submissions
mean_form_summary = forms_df[plot_cats['form_nums']+['total']].reset_index().groupby(['state_name']).mean()
mean_form_summary.loc['All States'] = forms_df_flat[plot_cats['form_nums']+['total']].mean()
logging.info(mean_form_summary.round(0))

# median number of form submissions
median_form_summary = forms_df[plot_cats['form_nums']+['total']].reset_index().groupby(['state_name']).median()
median_form_summary.loc['All States'] = forms_df_flat[plot_cats['form_nums']+['total']].median()
logging.info(median_form_summary.round(0))

# percent of form submissions
pct_form_summary = forms_df[plot_cats['form_nums']+['total']].reset_index().groupby(['state_name']).sum()
pct_form_summary.loc['All States'] = pct_form_summary.sum()
pct_form_summary = ff.divisor('pct', 'total', pct_form_summary, multiplier=100)
pct_form_summary = pct_form_summary[plot_cats['form_pct']]
logging.info(pct_form_summary.round(decimals=1))

# average form submission per day per worker
avrg_sub_per_summary = mean_form_summary.join(state_index)
cols = mean_form_summary.columns.tolist()
avrg_sub_per_summary.loc['All States', 'num_workers'] = avrg_sub_per_summary.loc[:,'num_workers'].sum()
for col in cols:
    avrg_sub_per_summary[col + '_per_aww_per_day'] = avrg_sub_per_summary[col] / avrg_sub_per_summary['num_workers']
avrg_sub_per_summary.loc['All States', 'num_workers'] = avrg_sub_per_summary.loc[:,'num_workers'].sum()
avrg_sub_per_summary = avrg_sub_per_summary.drop(cols, axis=1)
logging.info(avrg_sub_per_summary)

summary_df = pd.concat([mean_form_summary, median_form_summary, pct_form_summary, avrg_sub_per_summary], axis=1)
summary_df.to_csv('summary.csv')