# -*- coding: utf-8 -*-
"""
Real vs Fake Cases

@author: theism
"""
import re
import os
import logging
import pandas as pd
import gen_func
import case_func
import phone_func
import aadhar_func
import datetime
import numpy as np

# ----------------  USER EDITS -------------------------------
# Specify data location, regex to use on files, date columns that are dates
# Future - consider a GUI to do this input better

target_dir = (r'C:\Users\theism\Documents\Dimagi\Data\Case2')
output_dir = (r'C:\Users\theism\Documents\Dimagi\Results\Real Fake Cases')

# Practice Use Case on small dataset
# target_dir = (r'C:\Users\theism\Documents\Dimagi\Data\case_testing')
# output_dir = (r'C:\Users\theism\Documents\Dimagi\Data\case_testing')
# ------------- don't edit below here -----------------------------

# start logging
gen_func.start_logging(output_dir)

# identify all the states files to go through
folder_list = os.listdir(target_dir)
case_date_cols = ['opened_date', 'last_modified_date', 'dob']
case_data_regex = re.compile(r'cases_\d\d\d.csv')

# initialize outputs
output_dict = {}
output_df = pd.DataFrame()
output_df = output_df.fillna('')

logging.info('Looking in folders for data...')
logging.info('Starting scripts to analyze case data...')
for folder in folder_list:
    # initialize dataframe for output errors
    bad_df = pd.DataFrame()
    bad_df = bad_df.fillna('')

    if os.path.isdir(os.path.join(target_dir, folder)):
        location_name = gen_func.folder_name_to_location(folder)
        logging.info('-------------------------------------------')
        logging.info('Going through data for: %s' % location_name)
        logging.info('-------------------------------------------')
        output_dict = {'location': location_name}

        # combine all csv into one dataframe
        case_df = gen_func.csv_files_to_df(os.path.join(target_dir, folder), case_data_regex, case_date_cols)

        # clean case data - closed, orphan, test states/names, awc owner, blank names
        case_clean_df, output_dict = case_func.clean_case_data(case_df, output_dict)

        # add age distribution information to dataframe
        case_clean_df = case_func.add_age_info(case_clean_df,
                                               col_name='big_age_bracket',
                                               bin_type='brackets',
                                               relative_date='opened_date')
        case_clean_df = case_func.add_age_info(case_clean_df,
                                               col_name='small_age_bracket',
                                               bin_type='yearly',
                                               relative_date='opened_date')
        
        # add info about registration and last modified dates
        logging.info('HARDCODING DATE TO LAST DATE DATA EXPORTED: %s' % str(datetime.date(2017,8,8)))
        case_clean_df['reg_wi_30_days'] = (datetime.date(2017,8,8) - pd.to_datetime(
                case_clean_df['opened_date'], errors='coerce')) / np.timedelta64(1, 'D') <= 30
        case_clean_df['mod_wi_30_days'] = (datetime.date(2017,8,8) - pd.to_datetime(
                case_clean_df['last_modified_date'], errors='coerce')) / np.timedelta64(1, 'D') <= 30
        #for today, use datetime.date.today()
        #%%

        # see if the case is modified after the date the case is created
        # (indicates the case has been revisited / a form submitted against
        # the case on a subsequent date)
        opened_is_modified_index = case_clean_df['opened_date'] == case_clean_df['last_modified_date']
        case_clean_df['open_eq_mod'] = opened_is_modified_index
        open_mod = pd.DataFrame({'count': opened_is_modified_index.value_counts(),
                                 'pct': opened_is_modified_index.value_counts() / len(case_clean_df)})
        open_mod.loc['Total'] = open_mod.sum()
        logging.info('Clean cases not modified since opened:')
        logging.info(open_mod)
        if True in case_clean_df['reg_wi_30_days'].value_counts():
            logging.info('Cases opened within last 30 days: %i'
                     % case_clean_df['reg_wi_30_days'].value_counts()[True])
            if True in case_clean_df[case_clean_df['reg_wi_30_days'] == False]['mod_wi_30_days'].value_counts():
                logging.info('Cases not opened in last 30 but modified in last 30: %i'
                             % case_clean_df[case_clean_df['reg_wi_30_days'] == False]['mod_wi_30_days'].value_counts()[True])
            else:
                logging.info('No cases have been modified in last 30 days '
                             'that were opened before the last 30 days')
        else:
            logging.info('No cases registered within last 30 days')

        # see how many have an aadhar id that is unique
        logging.info('Seeing how many clean cases have a unique aadhar id')
        good_aadhar_df, bad_aadhar_df = aadhar_func.only_valid_aadhar(case_clean_df, 'aadhar_number')
        
        # see how many have an phone number that is unique
        logging.info('Seeing how many clean cases have a unique phone id')
        logging.info('REMINDER: phone numbers in app only collected for '
                     'females between 15 and 49 years of age.')
        # good_phone_df, bad_phone_df = phone_func.only_valid_phone(case_clean_df, 'contact_phone_number')


        # see how many 15-49 FEMALES have both a phone number and an aadhar number that is unique
        logging.info('Seeing how many FEMALES 15-49 years have both a valid aadhar and phone number')
        pregable_df = case_clean_df[case_clean_df['sex'] == 'F']
        pregable_df = pregable_df[pregable_df['big_age_bracket'] == '15-49 yrs']
        logging.info('%i cases that are Females 15-49 years in age' % len(pregable_df))
        good_pregable_df, bad_preg_aadhar_df = aadhar_func.only_valid_aadhar(pregable_df, 'aadhar_number')
        #good_pregable_df, bad_preg_phone_df = phone_func.only_valid_phone(good_pregable_df, 'contact_phone_number')
        logging.info('%i Females 15-49 years in age that also have valid '
                     'aadhar numbers and phone numbers (%i percent of %i 15-49 '
                     'Females)' % (len(good_pregable_df),
                                  100 * len(good_pregable_df) / len(pregable_df),
                                  len(pregable_df)))
        
        # see if the case is modified after the date the case is created for 
        # pregable df dataset
        preg_opened_is_modified_index = pregable_df['opened_date'] == pregable_df['last_modified_date']
        preg_open_mod = pd.DataFrame({'count': preg_opened_is_modified_index.value_counts(),
                                 'pct': preg_opened_is_modified_index.value_counts() / len(pregable_df)})
        preg_open_mod.loc['Total'] = preg_open_mod.sum()
        logging.info('Clean cases not modified since opened:')
        logging.info(preg_open_mod)
        
        # see how many 0-5 CHILDREN have both a phone number and an aadhar number that is unique
        logging.info('Seeing how many children under 5 years have a valid aadhar number')
        underfive_df = case_clean_df[case_clean_df['big_age_bracket'] == '0-5 yrs']
        logging.info('%i cases that are CHILDREN 0-5 years in age' % len(underfive_df))
        good_underfive_df, bad_underfive_aadh_df = aadhar_func.only_valid_aadhar(underfive_df, 'aadhar_number')
        logging.info('%i Children 0-5 years in age that have valid '
                     'aadhar numbers (%i percent of %i 0-5 '
                     'Females)' % (len(good_underfive_df),
                                  100 * len(good_underfive_df) / len(underfive_df),
                                  len(underfive_df)))
        
        # see if the case is modified after the date the case is created for 
        # children 0-5 df dataset
        child_opened_is_modified_index = underfive_df['opened_date'] == underfive_df['last_modified_date']
        child_open_mod = pd.DataFrame({'count': child_opened_is_modified_index.value_counts(),
                                 'pct': child_opened_is_modified_index.value_counts() / len(underfive_df)})
        child_open_mod.loc['Total'] = child_open_mod.sum()
        logging.info('Clean cases not modified since opened:')
        logging.info(child_open_mod)
        

        #%%
        # see distribution of population by age and gender
        # by gender
        logging.info('Distributions of ALL CLEAN cases by gender and age...')
        sex_counts = case_clean_df['sex'].value_counts()
        sex = pd.DataFrame({'count': sex_counts,
                            'pct': sex_counts / len(case_clean_df)})
        sex.loc['Total'] = sex.sum()
        logging.info('Value counts of cases by sex:')
        logging.info(sex)
        # by age (big buckets)
        age_counts = case_clean_df['big_age_bracket'].value_counts()
        age = pd.DataFrame({'count': age_counts,
                            'pct': age_counts / len(case_clean_df)})
        # age.loc['Total'] = age.sum()
        logging.info('Value counts of cases by age bucket:')
        logging.info(age)

        # by age/gender
        age_sex_counts = case_clean_df.groupby(['big_age_bracket', 'sex']).count()['caseid']
        age_sex = pd.DataFrame({'count': age_sex_counts,
                                'pct': age_sex_counts / len(case_clean_df)})
        # age_sex.loc['Total'] = age_sex.sum()
        logging.info('Distribution by age/sex:')
        logging.info(age_sex)

        # by age (small buckets)
        age_counts2 = case_clean_df['small_age_bracket'].value_counts()
        age_counts2.drop(['16-49 yrs', '49-99 yrs', '99 yrs+'])
        age2 = pd.DataFrame({'count': age_counts2,
                            'pct': age_counts2 / len(case_clean_df)})
        # age2.loc['Total'] = age2.sum()
        logging.info('Value counts of cases by small age bucket:')
        logging.info(age2)
        
        names_to_drop = ['5-6 yrs', '6-7 yrs', '7-8 yrs', '8-9 yrs', '9-10 yrs',
                         '10-11 yrs', '11-12 yrs', '12-13 yrs', '13-14 yrs',
                         '14-15 yrs', '15-16 yrs', '16-49 yrs', '49-99 yrs',
                         '99 yrs+']
        logging.info('Just 0-5:')
        logging.info(age2.drop(names_to_drop))
        
        # show some example caseIds to do rough checks on
        logging.info('these cases have GOOD aadhar numbers and are 15-49F and I think are real:')
        good_pregable_df.sample(n=15)
        logging.info('these cases have BAD aadhar numbers and are 15-49F and I think are less likely to be real:')
        bad_preg_aadhar_df.sample(n=15)
    
        # create summary output
        # output_df = output_df.append(output_dict, ignore_index=True)
