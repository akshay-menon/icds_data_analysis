# -*- coding: utf-8 -*-
"""
case load estimator - look at distributions of case loads

Created on Sat Jan 13 07:36:28 2018

@author: theism
"""
import os
import gen_func as gf
import pandas as pd
import re
import logging
import matplotlib.pyplot as plt
import seaborn as sns
from scipy import stats
from datetime import date

# define directories
# data_dir = r'C:\Users\theism\Documents\Dimagi\Data\testing'
data_dir = r'C:\Users\theism\Documents\Dimagi\Data\case_types'
output_dir = r'C:\Users\theism\Documents\Dimagi\Results\Case Load'

# refresh locations?
refresh_locations = True

# define case types
# data downloaded from https://www.icds-cas.gov.in/a/icds-cas/data/export/custom/daily_saved/
# [DA] <task_type> - make sure to check 'date updated' to see when data was refreshed
#case_types = ['ccs_record']
add_person_cases = True
case_types = ['child_health', 'tasks', 'ccs_record', 'measurement', 'household']
#case_types = ['tasks']
init_case_est = {'child_health':200,
                 'tasks':250,
                 'ccs_record':150,
                 'measurement':7000,
                 'household':170,
                 'person':1000}

# what file type in folders to get
case_data_regex = re.compile(r'Cases_\d\d\d.csv')
date_cols = []
cols_to_use = ['owner_id', 'closed']
location_columns = ['doc_id', 'block_name', 'district_name', 'state_name']
real_state_list = ['Madhya Pradesh', 'Chhattisgarh', 'Andhra Pradesh', 'Bihar',
                   'Jharkhand', 'Rajasthan']
# , 'Uttar Pradesh', 'Maharashtra']

# start logging
gf.start_logging(output_dir)

# initialize dfs
case_df = pd.DataFrame()
stats_df = pd.DataFrame()
stats_df = stats_df.fillna('')
closed_df = pd.DataFrame()
closed_df = closed_df.fillna('')

# get loads of each case type
# cycle through case types and create case_df
for case_type in case_types:
    logging.info('-------------------------------------------')
    logging.info('Going through data for: %s' % case_type)
    logging.info('-------------------------------------------')

    # get case dataset
    input_df = pd.DataFrame()
    input_df = case_df.fillna('')
    input_df = gf.csv_files_to_df(os.path.join(data_dir, case_type),
                                  case_data_regex, date_cols, cols_to_use)
    
    # input_df[case_type + '_open'] = input_df['closed'] == False
    # input_df[case_type + '_closed'] = input_df['closed'] != False
    
    # get all cases, open or closed.  closed matter for load testing since only
    # removed from phone if parent case closed
    input_df[case_type] = 1
    
    # show some open/closed info
    open_closed = input_df['closed'].value_counts()
    logging.info(open_closed)
    logging.info('Pct of %s open: %0.1f' % (case_type, (open_closed[False]*100./open_closed.sum())))
    closed_df.loc[:, case_type] = open_closed
    
    # groupby owner to collapse to series of awc_id and count 
    input_nums = input_df.drop('closed', axis=1).groupby(['owner_id']).sum()
    
    # add current cycle to the output
    case_df = pd.concat([case_df, input_nums], axis=1)
    
    logging.info(case_df.shape)

if add_person_cases:
    # get person cases separately, since have to download by each state
    person_case_df = pd.DataFrame()
    person_case_df = person_case_df.fillna('')
    person_target_dir = (r'C:\Users\theism\Documents\Dimagi\Data\Person_Case')
    folder_list = os.listdir(person_target_dir)
    person_case_data_regex = re.compile(r'cases_\d\d\d.csv')
    
    for folder in folder_list:
        if os.path.isdir(os.path.join(person_target_dir, folder)):
            location_name = gf.folder_name_to_location(folder)
            logging.info('-------------------------------------------')
            logging.info('Going through person cases for: %s' % location_name)
            logging.info('-------------------------------------------')
    
            # combine all csv into one dataframe
            person_input_df = gf.csv_files_to_df(os.path.join(person_target_dir, folder),
                                                 person_case_data_regex,
                                                 date_cols, cols_to_use)
    
            # only keep open cases
            # open_person_input_df = person_input_df[person_input_df['closed'] == False]
            person_input_df['person'] = 1
        
            # show some open/closed info
            open_closed = person_input_df['closed'].value_counts()
            logging.info(open_closed)
            logging.info('Pct of %s open: %0.1f' % (case_type, (open_closed[False]*100./open_closed.sum())))
            closed_df.loc[:, case_type + '-' + location_name] = open_closed
            
            # groupby owner to collapse to series of awc_id and count 
            person_input_df = person_input_df.drop('closed', axis=1).groupby(['owner_id']).sum()
        
            # add current cycle to the output
            person_case_df = person_case_df.append(person_input_df)
            
            logging.info(person_case_df.shape)
    
    
    # add the person case data to the existing df
    case_df = case_df.join(person_case_df)
    case_types.append('person')
case_df = case_df.fillna(0)
    
# get latest location fixture and add location data
if refresh_locations:
    gf.refresh_locations()
case_df = gf.add_locations(case_df, None, location_columns)
case_df = case_df.loc[(case_df['state_name'].isin(real_state_list))]

# get overall stats for case loads
overall_stats = case_df.describe()
logging.info(overall_stats)
overall_stats['state'] = 'all_data'
stats_df = stats_df.append(overall_stats)

# get distribution graphs for each type of case
# case_df.plot.hist(bins=100, alpha=0.2, xlim=(0,250)).set_title('Open Case Distribution by User')
axes = case_df.hist(figsize=[10, 8], bins=25)
plt.subplots_adjust(wspace=0.3, hspace=0.3)
plt.suptitle('Case Load - All AWW Users')
y = axes[0, 0].set_ylabel(ylabel='Frequency', labelpad=None)
y = axes[1, 0].set_ylabel(ylabel='Frequency', labelpad=None)
y = axes[2, 0].set_ylabel(ylabel='Frequency', labelpad=None)
x = axes[2, 0].set_xlabel(xlabel='Cases', labelpad=None)
x = axes[2, 1].set_xlabel(xlabel='Cases', labelpad=None)


case_melt = pd.melt(case_df.reset_index(), id_vars='index',value_vars=case_types, var_name='case_type', value_name='count')
case_melt = case_melt.set_index('index')
plt.figure()
sns.violinplot(x='case_type', y='count', data=case_melt, scale='count', inner='quartile')
plt.ylim([-100, 1250])
plt.title('Case Load - All AWW Users')

plt.figure()
ax = case_df.boxplot(figsize=[12, 8])
ax.set_yscale('log')
# TODO - cant get minor ticks off
ax.minorticks_on()
plt.title('Case Load - All AWW Users')

# break down above by state
for state in real_state_list:
    logging.info('-----------------------------')
    logging.info('Information for : %s' % state)
    logging.info('-----------------------------')
    temp_state_df = case_df[case_df['state_name'] == state]
    temp_stats = temp_state_df.describe()
    logging.info(temp_stats)
    temp_stats['state'] = state
    stats_df = stats_df.append(temp_stats)
    # TODO - add some sort of state wise plot
    # temp_state_df.plot.hist(bins=100, alpha=0.2, xlim=(0,500)).set_title('Open Case Distribution by User - %s' % state)

# plot by state
for state in real_state_list:
    temp_state_df = case_df[case_df['state_name'] == state]
    temp_case_melt = pd.melt(temp_state_df.reset_index(), id_vars='index',value_vars=case_types, var_name='case_type', value_name='count')
    temp_case_melt = temp_case_melt.set_index('index')
    plt.figure()
    sns.violinplot(x='case_type', y='count', data=temp_case_melt, scale='count', inner='quartile')
    plt.ylim([-100, 1250])
    plt.title('Case Load - %s' % state)
    
# see how initial estimates place in distributions
for case_type in case_types:
    percentile = stats.percentileofscore(case_df[case_type],init_case_est[case_type])
    logging.info('%s : %ith percentile (%i 100pct load case)' % (case_type, percentile, init_case_est[case_type]))
    percentile2 = stats.percentileofscore(case_df[case_type],init_case_est[case_type]*2)
    logging.info('%s : %ith percentile (%i 200pct load case)' % (case_type, percentile2, init_case_est[case_type]*2))

# write output files
case_df.to_csv(os.path.join(output_dir, 'case_load_by_aww_'  + date.today().strftime('%Y-%m-%d') + '.csv'))
stats_df.to_csv(os.path.join(output_dir, 'case_load_summary_'  + date.today().strftime('%Y-%m-%d') + '.csv'))
closed_df.to_csv(os.path.join(output_dir, 'open_closed_summary_'  + date.today().strftime('%Y-%m-%d') + '.csv'))
#