# -*- coding: utf-8 -*-
"""
Created on Sun Jul 01 09:41:09 2018

@author: theism
"""
import gen_func as gf
import logging
import re
import os
import pandas as pd
import numpy as np
from dateutil.parser import parse

def _see_if_on_sched(date_in, immun_buffer):
    if np.isnan(date_in):
        return 'null'
    elif date_in < (-1 * immun_buffer):
        return 'got_early'
    elif date_in > immun_buffer:
        return 'got_late'
    elif date_in == 0:
        return 'got_on_time'
    else:
        return 'got_in_buffer'

# ----------------  USER EDITS -------------------------------
# download date - this sets relative age for analysis
data_date = pd.Timestamp('05-29-2018')

# define directories
data_dir = gf.DATA_DIR + '\case_types'
output_dir = gf.OUTPUT_DIR + '\Pregnancy_tasks'

testing = True

# refresh locations?
refresh_locations = False

# buffer on either side of immun schedule date to consider good
immun_buffer = 7

# what file type in folders to get
case_data_regex = re.compile(r'Cases_\d\d\d.csv')
location_columns = ['doc_id', 'block_name', 'district_name', 'state_name']
real_state_list = ['Madhya Pradesh', 'Chhattisgarh', 'Andhra Pradesh', 'Bihar',
                   'Jharkhand', 'Rajasthan']
# , 'Uttar Pradesh', 'Maharashtra']

# ------------- don't edit below here -----------------------------
# start logging
gf.start_logging(output_dir)
os.chdir(output_dir)

# define immune schedule dates
preg_tasks = {
        'ANC 1 (immuns)': 0,
        'ANC 2 (immuns)': 0,
        'ANC 3 (immuns)': 42,
        'ANC 4 (immuns)': 42,
        'TT 1 (immuns)': 42,
        'TT 2 (immuns)': 42,
        'TT Booster (immuns)': 42
        }

# get tasks df
logging.info('Getting task case data')
task_date_cols = ['tt_complete_date', 'closed_date', 'last_modified_date', 'opened_date']
task_cols = task_date_cols + ['caseid', 'num_anc_complete', 'owner_id', 'schedule_flag', 'tasks_type',
                              'tt_complete', 'indices.ccs_record', 'closed', 'TT 1 (immuns)',
                              'TT 2 (immuns)',	'TT Booster (immuns)', 'ANC 1 (immuns)',
                              'ANC 2 (immuns)', 'ANC 3 (immuns)', 'ANC 4 (immuns)']
tasks_in_df = pd.DataFrame()
tasks_in_df = tasks_in_df.fillna('')
if testing:
    tasks_in_df = gf.forms_to_df(os.path.join(data_dir, 'tasks_TEST'), case_data_regex, task_date_cols, task_cols)
else:
    tasks_in_df = gf.forms_to_df(os.path.join(data_dir, 'tasks'), case_data_regex, task_date_cols, task_cols)
logging.info('Percentage of closed values out of %i cases' % tasks_in_df.shape[0])
logging.info(tasks_in_df['closed'].value_counts(dropna=False) * 100. / tasks_in_df.shape[0])
logging.info('Percentage of tasks by type out of %i cases' % tasks_in_df.shape[0])
logging.info(tasks_in_df['tasks_type'].value_counts(dropna=False) * 100. / tasks_in_df.shape[0])

# get latest location fixture and add location data
logging.info('Adding location data and removing test data ...')
if refresh_locations:
    gf.refresh_locations()
tasks_in_df = gf.add_locations(tasks_in_df, 'owner_id', location_columns)
tasks_in_df = tasks_in_df.loc[(tasks_in_df['state_name'].isin(real_state_list))]


#---------------------------------------------------------------------------------
#-------------------------- PREGNANCY IMMUNS -------------------------------------
#---------------------------------------------------------------------------------

preg_tasks_df = tasks_in_df[(tasks_in_df['tasks_type'] == 'pregnancy')]
num_preg = preg_tasks_df.shape[0]
logging.info('Analyzing %i pregnancy task cases' % num_preg)

logging.info(preg_tasks_df['closed'].value_counts(dropna=False) * 100. / num_preg)
logging.info(preg_tasks_df['schedule_flag'].value_counts(dropna=False) * 100. / num_preg)
logging.info(preg_tasks_df['tt_complete'].value_counts(dropna=False) * 100. / num_preg)

# link ccs_record data
logging.info('Loading ccs_record information for each case...')
ccs_df = pd.DataFrame()
ccs_df = ccs_df.fillna('')
ccs_date_cols = ['add', 'ccs_opened_date', 'edd', 'lmp', 'closed_date', 'last_modified_date',
                 'opened_date','bp1_date', 'bp2_date', 'bp3_date']
ccs_use_cols = ['caseid', 'last_preg_tt', 'num_bp_sched_visits'] + ccs_date_cols 
ccs_df = gf.forms_to_df(os.path.join(data_dir, 'ccs_record'), case_data_regex, ccs_date_cols, ccs_use_cols)
logging.info('Linking ccs info with task data...')
preg_tasks_df = pd.merge(preg_tasks_df, ccs_df, left_on='indices.ccs_record', right_on='caseid', how='left')

preg_tasks_df[0:200].to_csv('preg_test.csv')


'''
# ----------------------------------------------------------

# create columns to look at due vs done. first create lists for naming new columns
immun_list_dob = []
immun_list_sched = []
immun_list_code = []
for i in immun_list:
    immun_list_dob.append(i + '_after_dob')
    immun_list_sched.append(i + '_wrt_sched')
    immun_list_code.append(i + '_sched_code')
    
    logging.info('Adding columns to get days after dob for each immun...')
    for immun in immun_list:
        child_w_task_df[immun + '_after_dob'] = child_w_task_df[immun] - child_w_task_df['dob_unix_days']
    
    logging.info('Adding columns to compare immun date to schedule...')
    for immun in immun_list:
        child_w_task_df[immun + '_wrt_sched'] = child_w_task_df[immun + '_after_dob'] - immun_sched[immun]
    
    logging.info('Adding columns to code received immuns...(takes some time)')    
    for immun in immun_list:
        # logging.info('coding received %s' % immun)
        # if received, set time code, else set to 'null'
        child_w_task_df[immun + '_sched_code'] = child_w_task_df[immun + '_wrt_sched'].apply(lambda x: see_if_on_sched(x, immun_buffer))
    
    logging.info('Adding columns to code non-received immuns...(takes some time)')    
    for immun in immun_list: 
        # if is 'null', need to see if overdue or not due yet
        temp_series = (data_days - child_w_task_df['dob_unix_days'] - immun_sched[immun]) >= 0
        temp_series = temp_series.apply(lambda x: 'overdue' if x == True else 'not_due_yet')
        child_w_task_df[immun + '_sched_code'] = np.where(child_w_task_df[immun + '_sched_code'].eq('null'), temp_series.values, child_w_task_df[immun + '_sched_code'].values)
    
    # If Pentavalent path: Penta1/2/3, OPV1/2/3, BCG, Measles, VitA1
    # If DPT/HepB path: DPT1/2/3, HepB1/2/3, OPV1/2/3, BCG, Measles, VitA1
    logging.info('Adjusting coded values if immun not available or not on schedule path...')
    child_w_task_df['on_penta'] = (~np.isnan(child_w_task_df['Penta 1 (immuns)']) | ~np.isnan(child_w_task_df['Penta 2 (immuns)']) | ~np.isnan(child_w_task_df['Penta 3 (immuns)']))
    child_w_task_df['on_dpt'] = (~np.isnan(child_w_task_df['DPT 1 (immuns)']) | ~np.isnan(child_w_task_df['DPT 2 (immuns)']) | ~np.isnan(child_w_task_df['DPT 3 (immuns)']))
 

preg_tasks_df.iloc[0:200].to_csv(os.path.join(output_dir, 'preg_test.csv'))
# why add _x to caseid during merge?
# merge didn't work - check how do in other places.

# where does TT complete come from?  has 30% --- and 26% NaN
# how / need to link to previous preg TT
# how this preg case opened and closed in same form
#a12d4293-2584-4172-9e24-c00d9087f8e7
# link preg task cases to add
# link to dad cases?
# when does num_anc_complete get filled in?  30% ---
# what is the schedule for TT1/2 vs TTBooster?


# look at closed pregnancy cases - since have given birth (or died)
preg_closed_tasks_df = preg_tasks_df[(preg_tasks_df['closed'] == True)]
num_closed_preg = preg_closed_tasks_df.shape[0]
logging.info('Analyzing %i closed pregnancy task cases' % num_closed_preg)

preg_task_list = []
preg_task_list_short = []
for task in preg_tasks:
    short_task = task[:(len(task)-20)]
    preg_task_list.append(task)
    preg_task_list_short.append(short_task)
    
logging.info('Pct distribution of preg tasks:')
logging.info(preg_closed_tasks_df[preg_task_list].count(axis=0).sort(ascending=False) / num_closed_preg * 100.)  
logging.info('Num ANC complete for closed preg cases')
logging.info(preg_closed_tasks_df['num_anc_complete'].value_counts(dropna=False) * 100. / num_closed_preg)

# does a mean of num_anc_complete mean anything?  need to answer why 28% are --- and if those should be assigned to 0
# plot


# do same coding if can link to add for pregs
# TODO - fix preg_tasks schedule

    num_anc_complete
    schedule_flag
    tt_complete
    tt_complete_date
    open vs closed
    '''
