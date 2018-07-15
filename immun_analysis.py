# -*- coding: utf-8 -*-
"""
Created on Sun Feb 11 12:00:58 2018

@author: theism
"""
import gen_func as gf
import logging
import re
import os
import pandas as pd
import numpy as np
from itertools import cycle, islice
import matplotlib.pyplot as plt
from dateutil.parser import parse

def see_if_on_sched(date_in, immun_buffer):
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
    
def is_date(string):
    try: 
        parse(string)
        return True
    except ValueError:
        return False

    
# ----------------  USER EDITS -------------------------------
# download date - this sets relative age for analysis
data_date = pd.Timestamp('05-29-2018')

# define directories
data_dir = r'C:\Users\theism\Documents\Dimagi\Data\case_types'
output_dir = r'C:\Users\theism\Documents\Dimagi\Results\Immuns'
dob_dir = r'C:\Users\theism\Documents\Dimagi\Data\child_dob'

testing = False

# refresh locations?
refresh_locations = False

# download UCR task case data to link tasks to person date of birth?
download_dob = False

# buffer on either side of immun schedule date to consider good
immun_buffer = 7

# buffer to determine if counts as 'born in/with cas'
cas_delivered_buffer = 7

# run child and pregnancy analysis?
run_child = True
run_preg = False

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
immun_sched = {
        'BCG (immuns)': 0,
        'OPV 0 (immuns)': 0,
        'OPV 1 (immuns)': 42,
        'OPV 2 (immuns)': 70,
        'OPV 3 (immuns)': 98,
        'OPV Booster (immuns)': 480,
        'Penta 1 (immuns)': 42,
        'Penta 2 (immuns)': 70,
        'Penta 3 (immuns)': 98,
        'Hep B 0 (immuns)': 0,
        'Hep B 1 (immuns)': 42,
        'Hep B 2 (immuns)': 70,
        'Hep B 3 (immuns)': 98,
        'DPT 1 (immuns)': 42,
        'DPT 2 (immuns)': 70,
        'DPT 3 (immuns)': 98,
        'DPT Booster (immuns)': 480,
        'DPT Booster (immuns)1': 480,
        'DPT Booster 2 (immuns)': 1826,
        'Vitamin A 1 (immuns)': 273,
        'Vitamin A 2 (immuns)': 480,
        'Vitamin A 3 (immuns)': 730,
        'Vitamin A 4 (immuns)': 913,
        'Vitamin A 5 (immuns)': 1096,
        'Vitamin A 6 (immuns)': 1278,
        'Vitamin A 7 (immuns)': 1461,
        'Vitamin A 8 (immuns)': 1643,
        'Vitamin A 9 (immuns)': 1826,
        'Measles or MCV1 (immuns)': 273,
        'Measles 2 or MCV2 (immuns)': 480,
        'Rotavirus 1 (immuns)': 42,
        'Rotavirus 2 (immuns)': 70,
        'Rotavirus 3 (immuns)': 98,
        'JE 1 (immuns)': 273,
        'JE 2 (immuns)': 480,
        'IPV (immuns)': 98
        }

preg_tasks = {
        'ANC 1 (immuns)': 0,
        'ANC 2 (immuns)': 0,
        'ANC 3 (immuns)': 42,
        'ANC 4 (immuns)': 42,
        'TT 1 (immuns)': 42,
        'TT 2 (immuns)': 42,
        'TT Booster (immuns)': 42
        }
        

# make lists that will be used for iteration and columns
immun_one_year = [i for i in immun_sched if immun_sched[i] <=365]
immun_list = [i for i in immun_sched]

# get tasks df
logging.info('Getting task case data')
tasks_in_df = pd.DataFrame()
tasks_in_df = tasks_in_df.fillna('')
task_date_cols = ['immun_one_year_date', 'closed_date', 'last_modified_date', 'opened_date']
if testing:
    tasks_in_df = gf.forms_to_df(os.path.join(data_dir, 'tasks_TEST'), case_data_regex, task_date_cols)
else:
    tasks_in_df = gf.forms_to_df(os.path.join(data_dir, 'tasks'), case_data_regex, task_date_cols)
logging.info('Get only open task cases from non-test states')
logging.info('Percentage of closed values out of %i cases' % tasks_in_df.shape[0])
logging.info(tasks_in_df['closed'].value_counts(dropna=False) / tasks_in_df.shape[0])
logging.info('Percentage of tasks by type out of %i cases' % tasks_in_df.shape[0])
logging.info(tasks_in_df['tasks_type'].value_counts(dropna=False) / tasks_in_df.shape[0])

# get latest location fixture and add location data
logging.info('Adding location data and removing test data ...')
if refresh_locations:
    gf.refresh_locations()
tasks_in_df = gf.add_locations(tasks_in_df, 'owner_id', location_columns)
tasks_in_df = tasks_in_df.loc[(tasks_in_df['state_name'].isin(real_state_list))]


#---------------------------------------------------------------------------------
#-------------------------- CHILD IMMUNS -------------------------------------
#---------------------------------------------------------------------------------

if run_child:
    # only keep open cases, assign to dfs by task type
    child_tasks_df = tasks_in_df[(tasks_in_df['closed'] == False) & (tasks_in_df['tasks_type'] == 'child')]
    num_open_child = child_tasks_df.shape[0]
    logging.info('Analyzing %i open child task cases' % num_open_child)

    # link DOB
    logging.info('Collecting date of birth information for each child case...')
    if download_dob:
        location_file_dir = r'C:\Users\theism\Documents\Dimagi\Data\static-awc_location.csv'
        location_df = pd.read_csv(location_file_dir, index_col='doc_id', low_memory=False)
        unique_loc = location_df.drop_duplicates(subset='state_name')
        state_list = unique_loc['state_id'][unique_loc['state_name'].isin(real_state_list)].tolist()
        gf.iterate_ucr_download('static-icds-cas-static-tasks_cases', 'state_id', state_list, dob_dir)
    dob_date_cols = ['dob', 'date_turns_one_yr']
    dob_use_cols = ['doc_id', 'open_child_1yr_immun_complete', 'dob',
                    'date_turns_one_yr', 'open_child_count', 'is_migrated', 
                    'is_availing']
    dob_data_regex = re.compile(r'static-icds-cas-static-tasks_cases_\d+.csv')
    # know these files can get big.  split larger csvs.  Eventually wrap this into other function
    gf.find_and_split_csvs(dob_dir)
    child_dob_df = gf.forms_to_df(dob_dir, dob_data_regex, dob_date_cols, dob_use_cols)
    child_dob_df = child_dob_df.set_index('doc_id')
    logging.info('Merging dob info with immunization data...')
    child_tasks_df = pd.merge(child_tasks_df, child_dob_df, left_on='caseid',
                              right_index=True, how='left')
    child_tasks_df['num_immuns'] = child_tasks_df[immun_list].count(axis=1)
    
    # wanted to use the below, but due to incorrect phone times setting this date, we need to manually set it
    #data_date = child_tasks_df['last_modified_date'].max()
    logging.info('setting data date as max last modified date.  this determines relative age to dob.: %s' % data_date)
    child_tasks_df['age_days'] = (data_date - child_tasks_df['dob']) / np.timedelta64(1, 'D')
    child_tasks_df['dob_unix_days'] = (child_tasks_df['dob'] - pd.Timestamp('1-1-1970')) / np.timedelta64(1, 'D')

    # indicate if case born / tracked within CAS
    child_tasks_df['cas_delivered'] = (child_tasks_df['opened_date'] - child_tasks_df['dob']) / np.timedelta64(1, 'D') <= cas_delivered_buffer
    logging.info(child_tasks_df['cas_delivered'].value_counts())
    logging.info('number actually opened on day of delivery:')
    logging.info(((child_tasks_df['opened_date'] - child_tasks_df['dob']) / np.timedelta64(1, 'D') <= 1).sum())
    
    # look at information about who gets what immuns (but is all kids regardless age)      
    logging.info('Median age of child (in yrs): %.1f' % (child_tasks_df['age_days'].median() / 365.25))
    logging.info('Median number of immunizations: %.1f' % child_tasks_df['num_immuns'].median())
    logging.info('Mean number of immunizations: %.1f' % child_tasks_df['num_immuns'].mean())
    logging.info('Pct distribution of immunizations - top 5:')
    immun_out = pd.DataFrame()
    immun_dist_pct = child_tasks_df[immun_list].count(axis=0) / num_open_child * 100.
    immun_out = immun_dist_pct
    logging.info(immun_dist_pct.sort_values(ascending=False)[0:5])
    for state in real_state_list:
        logging.info(state)
        temp = child_tasks_df[child_tasks_df['state_name'] == state]
        temp_num_open_child = temp.shape[0]
        temp_immun_dist_pct = temp[immun_list].count(axis=0) / temp_num_open_child * 100.
        #logging.info(temp_immun_dist_pct.sort_values(ascending=False)[0:5])
        immun_out = pd.concat([immun_out, temp_immun_dist_pct.rename(state)], axis=1)
    
    immun_out.to_csv('immun_pcts.csv')
    
    # children with at least one immun
    child_tasks_df['immun_one_year_date_is_date'] = child_tasks_df['immun_one_year_date'].apply(is_date)
    logging.info('Pct child tasks with at least one immun:')
    child_w_task_df = child_tasks_df[child_tasks_df['num_immuns'] > 0].copy(False)
    num_child_w_1plus_task = child_w_task_df.shape[0]
    logging.info(num_child_w_1plus_task * 100. / num_open_child)    
    logging.info('Median age of child w/ at least one immun (in yrs): %.1f' % (child_w_task_df['age_days'].median() / 365.25))
    logging.info('Median number of immunizations w/ at least one immun: %.1f' % child_w_task_df['num_immuns'].median())
    logging.info('Mean number of immunizations w/ at least one immun: %.1f' % child_w_task_df['num_immuns'].mean())
    logging.info('Pct of children with case opened within %s days of dob:' % cas_delivered_buffer)
    logging.info(child_w_task_df['cas_delivered'].value_counts() * 100. / num_child_w_1plus_task)
    
    # children are at least one year of age - eligibility for this starts at 273 days
    num_child_over_one_yr = (child_tasks_df['age_days'] >= 273).sum()
    over_one_complete = ((child_tasks_df['age_days'] >= 273) & (child_tasks_df['immun_one_year_complete'] == 'yes')).sum()
    #under_one_complete = ((child_tasks_df['age_days'] < 365.25) & (child_tasks_df['immun_one_year_complete'] == 'yes')).sum()
    completed_eventually = ((child_tasks_df['age_days'] >= 273) & (child_tasks_df['immun_one_year_date_is_date'] == True)).sum()    
    logging.info('Children eligible for 1 yr immuns: %i (%0.1f of open children)' % (num_child_over_one_yr, (num_child_over_one_yr * 100. / num_open_child)))
    logging.info('Children eligible w/ all one year immuns on time: %i (%0.1f of eligible children)' % (over_one_complete, (over_one_complete * 100. / num_child_over_one_yr)))
    #logging.info('Children under one w/ all one year immuns: %i (%0.1f of open children)' % (under_one_complete, (under_one_complete * 100. / num_open_child)))
    logging.info('Children eligible w/ all one year immuns eventually: %i (%0.1f of eligible children)' % (completed_eventually, (completed_eventually * 100. / num_child_over_one_yr)))

    # REPEAT ABOVE IF CAS DELIVERED - children are at least one year of age - eligibility for this starts at 273 days
    cas_delivered_df = child_tasks_df[child_tasks_df['cas_delivered'] == True].copy(False)
    num_open_child_cas = cas_delivered_df.shape[0]
    num_child_over_one_yr_cas = (cas_delivered_df['age_days'] >= 273).sum()
    over_one_complete_cas = ((cas_delivered_df['age_days'] >= 273) & (cas_delivered_df['immun_one_year_complete'] == 'yes')).sum()
    completed_eventually_cas = ((cas_delivered_df['age_days'] >= 273) & (cas_delivered_df['immun_one_year_date_is_date'] == True)).sum()    
    logging.info('Children eligible for 1 yr immuns opened w/i 7 days of birth: %i (%0.1f of open children)' % (num_child_over_one_yr_cas, (num_child_over_one_yr_cas * 100. / num_open_child_cas)))
    logging.info('Children eligible w/ all one year immuns on time opened w/i 7 days of birth: %i (%0.1f of eligible children)' % (over_one_complete_cas, (over_one_complete_cas * 100. / num_child_over_one_yr_cas)))
    logging.info('Children eligible w/ all one year immuns eventually opened w/i 7 days of birth: %i (%0.1f of eligible children)' % (completed_eventually_cas, (completed_eventually_cas * 100. / num_child_over_one_yr_cas)))

    # REPEAT ABOVE FOR AT LEAST ONE IMMUN - children are at least one year of age - eligibility for this starts at 273 days
    num_child_over_one_yr_1plus = (child_w_task_df['age_days'] >= 273).sum()
    over_one_complete_1plus = ((child_w_task_df['age_days'] >= 273) & (child_w_task_df['immun_one_year_complete'] == 'yes')).sum()
    completed_eventually_1plus = ((child_w_task_df['age_days'] >= 273) & (child_w_task_df['immun_one_year_date_is_date'] == True)).sum()    
    logging.info('Children eligible for 1 yr immuns w/ 1plus immun: %i (%0.1f of 1plus children)' % (num_child_over_one_yr_1plus, (num_child_over_one_yr_1plus * 100. / num_child_w_1plus_task)))
    logging.info('Children eligible w/ all one year immuns on time opened w/i 7 days of birth: %i (%0.1f of eligible children)' % (over_one_complete_1plus, (over_one_complete_1plus * 100. / num_child_over_one_yr_1plus)))
    logging.info('Children eligible w/ all one year immuns eventually opened w/i 7 days of birth: %i (%0.1f of eligible children)' % (completed_eventually_1plus, (completed_eventually_1plus * 100. / num_child_over_one_yr_1plus)))

      
    
    for state in real_state_list:
        temp = child_tasks_df[child_tasks_df['state_name'] == state]
        temp_num_open_child = temp.shape[0]
        if temp_num_open_child > 0:
            temp_num_child_over_one_yr = (temp['age_days'] >= 273).sum()
            temp_over_one_complete = ((temp['age_days'] >= 273) & (temp['immun_one_year_complete'] == 'yes')).sum()
            #temp_under_one_complete = ((temp['age_days'] < 365.25) & (temp['immun_one_year_complete'] == 'yes')).sum()
            temp_completed_eventually = ((temp['age_days'] >= 273) & (temp['immun_one_year_date_is_date'] == True)).sum()    
            logging.info('%i children in %s (%0.1f pct of open children)' % (temp_num_open_child, state, temp_num_open_child * 100. / num_open_child))
            logging.info('Children eligible for 1 yr immuns: %i (%0.1f pct of open children in state)' % (temp_num_child_over_one_yr, (temp_num_child_over_one_yr * 100. / temp_num_open_child)))
            logging.info('Children eligible w/ all one year immuns on time: %i (%0.1f pct of eligible children in state)' % (temp_over_one_complete, (temp_over_one_complete * 100. / temp_num_child_over_one_yr)))
            #logging.info('Children under one w/ all one year immuns: %i (%0.1f of pct open children in state)' % (temp_under_one_complete, (temp_under_one_complete * 100. / temp_num_open_child)))
            logging.info('Children eligible w/ all one year immuns eventually: %i (%0.1f pct of eligible children in state)' % (temp_completed_eventually, (temp_completed_eventually * 100. / temp_num_child_over_one_yr)))

    # cases that have je available
    logging.info(child_w_task_df['je_available'].value_counts(dropna=False) * 100. / num_child_w_1plus_task)
    # cases that have rv available
    logging.info(child_w_task_df['rv_available'].value_counts(dropna=False) * 100. / num_child_w_1plus_task)
    # % on penta path vs other
    logging.info(child_w_task_df['penta_path'].value_counts(dropna=False) * 100. / num_child_w_1plus_task)
    # % on penta path vs other
    logging.info(child_w_task_df['schedule_flag'].value_counts(dropna=False) * 100. / num_child_w_1plus_task)
    
    # add days since dob column to compare to immun values
    logging.info('Adding columns to compare immuns to dob...')
    data_days = (data_date - pd.Timestamp('1-1-1970')) / np.timedelta64(1, 'D')
    
    # create lists for naming new columns
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
    logging.info('Adjusting coded values if immun not available or not on schedule path...''')
    child_w_task_df['on_penta'] = (~np.isnan(child_w_task_df['Penta 1 (immuns)']) | ~np.isnan(child_w_task_df['Penta 2 (immuns)']) | ~np.isnan(child_w_task_df['Penta 3 (immuns)']))
    child_w_task_df['on_dpt'] = (~np.isnan(child_w_task_df['DPT 1 (immuns)']) | ~np.isnan(child_w_task_df['DPT 2 (immuns)']) | ~np.isnan(child_w_task_df['DPT 3 (immuns)']))
    child_w_task_df['on_hepb'] = (~np.isnan(child_w_task_df['Hep B 1 (immuns)']) | ~np.isnan(child_w_task_df['Hep B 2 (immuns)']) | ~np.isnan(child_w_task_df['Hep B 3 (immuns)']))
    child_w_task_df['on_dpt_hepb'] = (child_w_task_df['on_dpt'] | child_w_task_df['on_hepb'])
    child_w_task_df['on_both'] = (child_w_task_df['on_penta'] & child_w_task_df['on_dpt_hepb'])
    child_w_task_df['on_none'] = ((child_w_task_df['on_penta'] == False) & (child_w_task_df['on_dpt_hepb'] == False))
    child_w_task_df['JE 1 (immuns)_sched_code'] = np.where(child_w_task_df['je_available'].eq('no'), 'not_on_path', child_w_task_df['JE 1 (immuns)_sched_code'].values)
    child_w_task_df['JE 2 (immuns)_sched_code'] = np.where(child_w_task_df['je_available'].eq('no'), 'not_on_path', child_w_task_df['JE 2 (immuns)_sched_code'].values)
    child_w_task_df['Rotavirus 1 (immuns)_sched_code'] = np.where(child_w_task_df['rv_available'].eq('no'), 'not_on_path', child_w_task_df['Rotavirus 1 (immuns)_sched_code'].values)
    child_w_task_df['Rotavirus 2 (immuns)_sched_code'] = np.where(child_w_task_df['rv_available'].eq('no'), 'not_on_path', child_w_task_df['Rotavirus 2 (immuns)_sched_code'].values)
    child_w_task_df['Rotavirus 3 (immuns)_sched_code'] = np.where(child_w_task_df['rv_available'].eq('no'), 'not_on_path', child_w_task_df['Rotavirus 3 (immuns)_sched_code'].values)
    child_w_task_df['Penta 1 (immuns)_sched_code'] = np.where(child_w_task_df['on_dpt_hepb'].eq(True), 'not_on_path', child_w_task_df['Penta 1 (immuns)_sched_code'].values)
    child_w_task_df['Penta 2 (immuns)_sched_code'] = np.where(child_w_task_df['on_dpt_hepb'].eq(True), 'not_on_path', child_w_task_df['Penta 2 (immuns)_sched_code'].values)
    child_w_task_df['Penta 3 (immuns)_sched_code'] = np.where(child_w_task_df['on_dpt_hepb'].eq(True), 'not_on_path', child_w_task_df['Penta 3 (immuns)_sched_code'].values)
    child_w_task_df['DPT Booster (immuns)1_sched_code'] = np.where(child_w_task_df['on_dpt_hepb'].eq(True), 'not_on_path', child_w_task_df['DPT Booster (immuns)1_sched_code'].values)
    child_w_task_df['DPT 1 (immuns)_sched_code'] = np.where(child_w_task_df['on_penta'].eq(True), 'not_on_path', child_w_task_df['DPT 1 (immuns)_sched_code'].values)
    child_w_task_df['DPT 2 (immuns)_sched_code'] = np.where(child_w_task_df['on_penta'].eq(True), 'not_on_path', child_w_task_df['DPT 2 (immuns)_sched_code'].values)
    child_w_task_df['DPT 3 (immuns)_sched_code'] = np.where(child_w_task_df['on_penta'].eq(True), 'not_on_path', child_w_task_df['DPT 3 (immuns)_sched_code'].values)
    child_w_task_df['Hep B 1 (immuns)_sched_code'] = np.where(child_w_task_df['on_penta'].eq(True), 'not_on_path', child_w_task_df['Hep B 1 (immuns)_sched_code'].values)
    child_w_task_df['Hep B 2 (immuns)_sched_code'] = np.where(child_w_task_df['on_penta'].eq(True), 'not_on_path', child_w_task_df['Hep B 2 (immuns)_sched_code'].values)
    child_w_task_df['Hep B 3 (immuns)_sched_code'] = np.where(child_w_task_df['on_penta'].eq(True), 'not_on_path', child_w_task_df['Hep B 3 (immuns)_sched_code'].values)
    #DPT Booster (immuns) is for DPT path, DPT Booster (immuns)1 is for penta path
    child_w_task_df['DPT Booster (immuns)_sched_code'] = np.where(child_w_task_df['on_penta'].eq(True), 'not_on_path', child_w_task_df['DPT Booster (immuns)_sched_code'].values)
    child_w_task_df['DPT Booster (immuns)1_sched_code'] = np.where(child_w_task_df['on_dpt_hepb'].eq(True), 'not_on_path', child_w_task_df['DPT Booster (immuns)1_sched_code'].values)
    
    #child_on_both_paths = child_w_task_df[child_w_task_df['on_both'] | child_w_task_df['on_none']]
    #child_on_both_paths.to_csv('child_tasks_bothnone_dpt_penta.csv')
    logging.info('Case distribution on both Penta path and DPT/HepB path:')
    logging.info(child_w_task_df['on_both'].value_counts(dropna=False) / num_child_w_1plus_task)
    logging.info('Case distribution on neither Penta path nor DPT/HepB path:')
    logging.info(child_w_task_df['on_none'].value_counts(dropna=False) / num_child_w_1plus_task)
    
    # get real penta data
    #logging.info('Pct of cases with defined value for penta_path')
    #penta_info = child_w_task_df[(child_w_task_df['penta_path'] == 'dpt_hepb') | (child_w_task_df['penta_path'] == 'penta')]
    #logging.info(penta_info['penta_path'].value_counts() * 100. / penta_info.sum())

    logging.info('compared to penta info based on immun records')
    child_on_some_sched = (child_w_task_df['on_none'] != True).sum()
    logging.info('Pct on Penta out of on some sched: %0.1f' % (child_w_task_df['on_penta'].sum() * 100. / child_on_some_sched))
    logging.info('Pct on DPT / HepB out of on some sched: %0.1f' % (child_w_task_df['on_dpt_hepb'].sum() * 100. / child_on_some_sched))
    logging.info('Pct on None: %0.1f' % (child_w_task_df['on_none'].sum() * 100. / num_child_w_1plus_task))
    for state in real_state_list:
        temp = child_w_task_df[child_w_task_df['state_name'] == state]
        denom2 = temp.shape[0]
        denom1 = (temp['on_none'] != True).sum()
        if denom2 != 0:
            penta = temp['on_penta'].sum() * 100. / denom1
            none = temp['on_none'].sum() * 100. / denom1
            dpt = temp['on_dpt_hepb'].sum() * 100. / denom2
            logging.info('%s: Penta: %0.1f  DPT: %0.1f  None: %0.1f' % (state, penta, dpt, none))

    child_w_task_df.iloc[0:200].to_csv('test.csv')
    
    # create summary table
    row_order = ['not_on_path', 'not_due_yet', 'overdue', 'got_early', 'got_late', 'got_on_time', 'got_in_buffer']
    summary_df = child_w_task_df[immun_list_code].apply(pd.Series.value_counts).fillna(0)
    summary_df = summary_df.reindex(row_order)
    
    cas_delivered_summary = cas_delivered_df[immun_list_code].apply(pd.Series.value_counts).fillna(0)
    cas_delivered_summary = cas_delivered_summary.reindex(row_order)
    
    # shorten names
    orig_names = summary_df.columns.tolist()
    new_name_dict = {}
    immun_list_short = []
    for i in orig_names:
        immun = i[:(len(i)-20)]
        new_name_dict[i] = immun
        immun_list_short.append(immun)
    summary_df = summary_df.rename(columns = new_name_dict)
    cas_delivered_summary = cas_delivered_summary.rename(columns = new_name_dict)
    cas_delivered_summary.loc['Total'] = cas_delivered_summary.sum()
    
    summary_pct_df = summary_df / summary_df.sum() * 100
    # for plotting and other analysis, remove immuns not due yet or expected to be due
    onpath_summary_df = summary_df.drop(['not_due_yet', 'not_on_path'])
    onpath_summary_pct_df = onpath_summary_df / onpath_summary_df.sum() * 100
    summary_df.loc['Total'] = summary_df.sum()
    
    # output a summary file
    summary_df.loc['received'] = summary_df.loc['got_early'] + summary_df.loc['got_late'] + summary_df.loc['got_on_time'] + summary_df.loc['got_in_buffer']
    summary_df.loc['should_have_received'] = summary_df.loc['received'] + summary_df.loc['overdue']
    summary_df.loc['pct_received'] = summary_df.loc['received'] * 100. / summary_df.loc['should_have_received']

    cas_delivered_summary.loc['received'] = cas_delivered_summary.loc['got_early'] + cas_delivered_summary.loc['got_late'] + cas_delivered_summary.loc['got_on_time'] + cas_delivered_summary.loc['got_in_buffer']
    cas_delivered_summary.loc['should_have_received'] = cas_delivered_summary.loc['received'] + cas_delivered_summary.loc['overdue']
    cas_delivered_summary.loc['pct_received'] = cas_delivered_summary.loc['received'] * 100. / cas_delivered_summary.loc['should_have_received']
    
    summary_file = os.path.join(output_dir, 'summary.csv')
    summary_df.to_csv(summary_file)
    logging.info('output file saved to %s' % summary_file)
    
    cas_delivered_summary_file = os.path.join(output_dir, 'summary_cas_delivered.csv')
    cas_delivered_summary.to_csv(cas_delivered_summary_file)
    
    # make a plot of summary data
    pct_plot_df = onpath_summary_pct_df.transpose()
    my_colors = list(islice(cycle(['lightgrey', 'orange', 'salmon', 'green', 'lightgreen']), None, len(pct_plot_df)))
    immun_list_short.sort()
    pct_plot_df = pct_plot_df.reindex(immun_list_short)
    pct_plot_df.plot(kind='bar',
                    figsize=[16, 6],
                    stacked=True,
                    colors=my_colors,
                    title='Vaccine Schedule Performance for Children w/ 1+ Immun')
    plt.tight_layout()
    
    immun_one_year_short = [i[:(len(i)-9)] for i in immun_one_year]
    immun_one_year_short.sort()
    pct_plot_df.loc[immun_one_year_short].plot(kind='bar',
                    figsize=[16, 6],
                    stacked=True,
                    colors=my_colors,
                    title='Vaccine Schedule Performance for Children w/ 1+ Immun - Immuns Due by 1 Year')
    plt.tight_layout()
    
    logging.info('Received / Should have received by immun:')
    logging.info(summary_df.loc['pct_received'].sort_values(ascending=False).round(1))
