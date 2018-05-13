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

    
# ----------------  USER EDITS -------------------------------
# date data was downloaded (can probably get this from file in a better way...)
data_date = start_date = pd.Timestamp('02-13-2018')        

# define directories
data_dir = r'C:\Users\theism\Documents\Dimagi\Data\case_types'
output_dir = r'C:\Users\theism\Documents\Dimagi\Results\Immuns'
dob_dir = r'C:\Users\theism\Documents\Dimagi\Data\child_dob'

testing = True

# refresh locations?
refresh_locations = False

# download UCR task case data to link tasks to person date of birth?
download_dob = False

# buffer on either side of immun schedule date to consider good
immun_buffer = 7

# run child and pregnancy analysis?
run_child = True
run_preg = True

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
    tasks_in_df = gf.csv_files_to_df(os.path.join(data_dir, 'tasks_TEST'), case_data_regex, task_date_cols)
else:
    tasks_in_df = gf.csv_files_to_df(os.path.join(data_dir, 'tasks'), case_data_regex, task_date_cols)
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
    dob_data_regex = re.compile(r'static-icds-cas-static-tasks_cases_\d.csv')
    child_dob_df = gf.csv_files_to_df(dob_dir, dob_data_regex, dob_date_cols, dob_use_cols)
    child_dob_df = child_dob_df.set_index('doc_id')
    logging.info('Merging dob info with immunization data...')
    child_tasks_df = pd.merge(child_tasks_df, child_dob_df, left_on='caseid',
                              right_index=True, how='left')
        
    # look at information about who gets what immuns (but is all kids regardless age)
    logging.info('Pct distribution of all immunizations:')
    logging.info(child_tasks_df[immun_list].count(axis=0) / num_open_child * 100.)    
    child_tasks_df['age_days'] = (data_date - child_tasks_df['dob']) / np.timedelta64(1, 'D')
    child_tasks_df['dob_unix_days'] = (child_tasks_df['dob'] - pd.Timestamp('1-1-1970')) / np.timedelta64(1, 'D')
    
    child_tasks_df['num_immuns'] = child_tasks_df[immun_list].count(axis=1)
    logging.info('Pct child tasks with at least one immun:')
    child_w_task_df = child_tasks_df[child_tasks_df['num_immuns'] > 0]
    num_child_w_1plus_task = child_w_task_df.shape[0]
    logging.info(num_child_w_1plus_task * 100. / num_open_child)
    
    logging.info('Median age of child (in yrs): %.1f' % (child_tasks_df['age_days'].median() / 365.25))
    logging.info('Median number of immunizations: %.1f' % child_tasks_df['num_immuns'].median())
    logging.info('Mean number of immunizations: %.1f' % child_tasks_df['num_immuns'].mean())
    logging.info('Median age of child w/ at least one immun (in yrs): %.1f' % (child_w_task_df['age_days'].median() / 365.25))
    logging.info('Median number of immunizations w/ at least one immun: %.1f' % child_w_task_df['num_immuns'].median())
    logging.info('Mean number of immunizations w/ at least one immun: %.1f' % child_w_task_df['num_immuns'].mean())
    
    # children are at least one year of age
    num_child_over_one_yr = (child_tasks_df['age_days'] >= 365.25).sum()
    over_one_complete = ((child_tasks_df['age_days'] >= 365.25) & (child_tasks_df['immun_one_year_complete'] == 'yes')).sum()
    under_one_complete = ((child_tasks_df['age_days'] < 365.25) & (child_tasks_df['immun_one_year_complete'] == 'yes')).sum()
    logging.info('Children over one year: %i (%0.1f of open children)' % (num_child_over_one_yr, (num_child_over_one_yr * 100. / num_open_child)))
    logging.info('Children over one w/ all one year immuns: %i (%0.1f of open children)' % (over_one_complete, (over_one_complete * 100. / num_open_child)))
    logging.info('Children under one w/ all one year immuns: %i (%0.1f of open children)' % (under_one_complete, (under_one_complete * 100. / num_open_child)))
    
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
    
    child_on_both_paths = child_w_task_df[child_w_task_df['on_both'] | child_w_task_df['on_none']]
    child_on_both_paths.to_csv('child_tasks_bothnone_dpt_penta.csv')
    logging.info('Case distribution on both Penta path and DPT/HepB path:')
    logging.info(child_w_task_df['on_both'].value_counts(dropna=False) / num_child_w_1plus_task)
    logging.info('Case distribution on neither Penta path nor DPT/HepB path:')
    logging.info(child_w_task_df['on_none'].value_counts(dropna=False) / num_child_w_1plus_task)
        
    logging.info('Pct on Penta: %0.1f' % (child_w_task_df['on_penta'].sum() * 100. / num_child_w_1plus_task))
    logging.info('Pct on DPT / HepB: %0.1f' % (child_w_task_df['on_dpt_hepb'].sum() * 100. / num_child_w_1plus_task))
    logging.info('Pct on None: %0.1f' % (child_w_task_df['on_none'].sum() * 100. / num_child_w_1plus_task))
    for state in real_state_list:
        temp = child_w_task_df[child_w_task_df['state_name'] == state]
        denom = temp.shape[0]
        if denom != 0:
            penta = temp['on_penta'].sum() * 100. / denom
            none = temp['on_none'].sum() * 100. / denom
            dpt = temp['on_dpt_hepb'].sum() * 100. / denom
            logging.info('%s: Penta: %0.1f  DPT: %0.1f  None: %0.1f' % (state, penta, dpt, none))

    child_w_task_df.iloc[0:200].to_csv('test.csv')
    
    # create summary table
    row_order = ['not_on_path', 'overdue', 'not_due_yet', 'got_early', 'got_late', 'got_on_time', 'got_in_buffer']
    summary_df = child_w_task_df[immun_list_code].apply(pd.Series.value_counts).fillna(0)
    summary_df = summary_df.reindex(row_order)
    
    # shorten names
    orig_names = summary_df.columns.tolist()
    new_name_dict = {}
    immun_list_short = []
    for i in orig_names:
        immun = i[:(len(i)-20)]
        new_name_dict[i] = immun
        immun_list_short.append(immun)
    summary_df = summary_df.rename(columns = new_name_dict)
    
    summary_pct_df = summary_df / summary_df.sum() * 100
    # for plotting and other analysis, remove immuns not due yet or expected to be due
    onpath_summary_df = summary_df.drop(['not_due_yet', 'not_on_path'])
    onpath_summary_pct_df = onpath_summary_df / onpath_summary_df.sum() * 100
    summary_df.loc['Total'] = summary_df.sum()
    summary_df = summary_df.append(onpath_summary_pct_df)
    
    # output a summary file
    summary_df.loc['received'] = summary_df.loc['got_early'] + summary_df.loc['got_late'] + summary_df.loc['got_on_time'] + summary_df.loc['got_in_buffer']
    summary_df.loc['should_have_received'] = summary_df.loc['received'] + summary_df.loc['overdue']
    summary_df.loc['pct_received'] = summary_df.loc['received'] * 100. / summary_df.loc['should_have_received']
    
    summary_file = os.path.join(output_dir, 'summary.csv')
    summary_df.to_csv(summary_file)
    logging.info('output file saved to %s' % summary_file)
    
    # make a plot of summary data
    pct_plot_df = onpath_summary_pct_df.transpose()
    immun_list_short.sort()
    pct_plot_df = pct_plot_df.reindex(immun_list_short)
    pct_plot_df.plot(kind='bar',
                    figsize=[16, 6],
                    stacked=True,
                    title='Activity for all Immuns')
    
    immun_one_year_short = [i[:(len(i)-9)] for i in immun_one_year]
    immun_one_year_short.sort()
    pct_plot_df.loc[immun_one_year_short].plot(kind='bar',
                    figsize=[16, 6],
                    stacked=True,
                    title='Activity for Immuns Due Before 1 Year of Age')

    logging.info('Received / Should have received by immun:')
    logging.info(summary_df.loc['pct_received'].sort_values(ascending=False).round(1))
    
    

#---------------------------------------------------------------------------------
#-------------------------- PREGNANCY IMMUNS -------------------------------------
#---------------------------------------------------------------------------------

if run_preg:
    # only keep open cases, assign to dfs by task type
    preg_tasks_df = tasks_in_df[(tasks_in_df['tasks_type'] == 'pregnancy')]
    num_preg = preg_tasks_df.shape[0]
    logging.info('Analyzing %i pregnancy task cases' % num_preg)
    
    logging.info(preg_tasks_df['closed'].value_counts(dropna=False) * 100. / num_preg)
    logging.info(preg_tasks_df['schedule_flag'].value_counts(dropna=False) * 100. / num_preg)
    logging.info(preg_tasks_df['tt_complete'].value_counts(dropna=False) * 100. / num_preg)
    
    preg_tasks_df.iloc[0:200].to_csv('preg_test.csv')
    
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



#----------------------





want to plot a heat map, where one axis is immun name, other is coding
#--------------------------------
child_w_task_df.iloc[0:100].to_csv('test.csv')


    
    logging.info(immun)
    child_w_task_df[immun + '_time'] = child_w_task_df.apply(lambda x: see_if_on_sched(x[immun + '_after_dob'], immun, immun_sched, immun_buffer), axis=1)


    


schedule distribution
groupby aww users.  are some users adding vaccinations and others not?
get avrg number of children w/ at least 1 immun per user
get avrg number of children w/ at least 5 immuns per user
get avrg number of children over 1 year with completed immuns per user
look at distribution of above
check immun types by age (https://confluence.dimagi.com/display/ICDS/Due+List+Details)

# avrg number of immuns at one year (how close to full immun?)
is a task case created automatically?  is it the same as number of open children?

mothers
when delivered, distribution of ANC visits
tt complete



preg_tasks_df = tasks_in_df[(tasks_in_df['closed'] == False) & (tasks_in_df['tasks_type'] == 'pregnancy')]
preg_tasks_df = gf.add_locations(preg_tasks_df, 'owner_id', location_columns)
preg_tasks_df = preg_tasks_df.loc[(preg_tasks_df['state_name'].isin(real_state_list))]
# questions for further study:
# - how are immuns getting updated?  due list or hh forms? (more extensive analysis...)
# - further investigation of why closed in future.  removed household, orphan cases need to look at outcomes with death

# not sure if need these anymore
# define various lists of columns for iteration
non_immun_cols = ['block_name',
                 'district_name',
                 'state_name',
                 'number',
                 'caseid',
                 'case',
                 'due_list_max_expires',
                 'due_list_min_eligible',
                 'immun_one_year_complete',
                 'immun_one_year_date',
                 'je_available',
                 'num_anc_complete',
                 'num_immun_done_one_year',
                 'owner_id',
                 'penta_path',
                 'rv_available',
                 'schedule_flag',
                 'tasks_type',
                 'tt_complete',
                 'tt_complete_date',
                 'indices.ccs_record',
                 'indices.child_health',
                 'closed',
                 'closed_date',
                 'last_modified_date',
                 'opened_date']

# If Pentavalent path: Penta1/2/3, OPV1/2/3, BCG, Measles, VitA1
penta_one_yr_immuns = ['BCG (immuns)',
                       'OPV 1 (immuns)',
                       'OPV 2 (immuns)',
                       'OPV 3 (immuns)',
                       'Penta 1 (immuns)',
                       'Penta 2 (immuns)',
                       'Penta 3 (immuns)',
                       'Vitamin A 1 (immuns)',
                       'Measles or MCV1 (immuns)']

# If DPT/HepB path: DPT1/2/3, HepB1/2/3, OPV1/2/3, BCG, Measles, VitA1
nopenta_one_yr_immuns = ['BCG (immuns)',
                         'OPV 1 (immuns)',
                         'OPV 2 (immuns)',
                         'OPV 3 (immuns)',
                         'Hep B 1 (immuns)',
                         'Hep B 2 (immuns)',
                         'Hep B 3 (immuns)',
                         'DPT 1 (immuns)',
                         'DPT 2 (immuns)',
                         'DPT 3 (immuns)',
                         'Vitamin A 1 (immuns)',
                         'Measles or MCV1 (immuns)']
child_w_task_df['date_since_mod'] = (child_w_task_df['last_modified_date'] - pd.Timestamp('1-1-1970')) / np.timedelta64(1, 'D')
child_w_task_df['days_since_mod'] = child_w_task_df['date_since_mod'] - child_w_task_df['days_since_dob']

