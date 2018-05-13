# -*- coding: utf-8 -*-
"""
Created on Tue May 01 08:00:12 2018

@author: theism
"""

import re
import os
import pandas as pd
import gen_func as gf
import logging
import numpy as np
import matplotlib.pyplot as plt

# ----------------  USER EDITS -------------------------------

# input directories and information
target_dir = 'C:\\Users\\theism\\Documents\\Dimagi\\Data\\new_forms'
data_regex = re.compile(r'Forms_\d\d\d.csv')
output_dir = 'C:\\Users\\theism\\Documents\\Dimagi\\Results\\Videos'

# states we care about
real_state_list = ['Madhya Pradesh', 'Chhattisgarh',
                   'Andhra Pradesh', 'Bihar', 'Jharkhand',
                   'Rajasthan']
#, 'Uttar Pradesh', 'Maharashtra'] 

video_list = ['form.counseling.bp_videos','form.counseling.complementary_feeding',
              'form.counseling.is_family_planning','form.counseling.is_sanitation',
              'form.training.show_gmp','form.training.show_hvs','form.training.show_thr',
              'form.training.show_daily_feeding','form.training.show_due_list',
              'form.training.show_hh_reg']

## ------------- don't edit below here -----------------------------

# start logging
gf.start_logging(output_dir)
vid_date_cols = ['received_on', 'completed_time', 'started_time']
os.chdir(output_dir)
        
# BP form info
bp_dir = os.path.join(target_dir, '[DA] Birth Preparedness - min_video')
bp_df = gf.csv_files_to_df(bp_dir, data_regex, date_cols = ['completed_time'])
logging.info('raw BP forms: %i' % bp_df.shape[0])
bp_df = gf.add_locations_by_username(bp_df)
logging.info('raw BP forms after add locations: %i' % bp_df.shape[0])
bp_df = bp_df.loc[(bp_df['state_name'].isin(real_state_list))]
num_bp_forms = bp_df.shape[0]
logging.info('Num Birth Prep forms in real locations: %i' % num_bp_forms)
logging.info('%i different users submitted this form' % bp_df['awc_name'].nunique())
logging.info('%.2f average forms per user' % bp_df['awc_name'].value_counts().mean())
# try to also coordinate with birth phase
bp_df['completed_time'] = pd.to_datetime(bp_df['completed_time'])
bp_df['started_time'] = pd.to_datetime(bp_df['started_time'])
bp_df['form_duration'] = (bp_df['completed_time'] - bp_df['started_time']) / np.timedelta64(1, 'm')
bp_df['form.cur_edd'] = pd.to_datetime(bp_df['form.cur_edd'])
bp_df['days_to_edd'] = (bp_df['form.cur_edd'] - bp_df['completed_time']) / np.timedelta64(1, 'D')
bp_df['trimester'] = bp_df['days_to_edd'].apply(lambda x: 'third' if x <= 91 else ('second' if x <= 182 else 'first'))
logging.info('Distribution of BP forms by trimester:')
logging.info(bp_df['trimester'].value_counts(dropna=False) * 100. / num_bp_forms)
bp_df['form.bp2.play_birth_preparedness_vid'] = bp_df['form.bp2.play_birth_preparedness_vid'].fillna('skipped')
bp_df['form.play_family_planning_vid'] = bp_df['form.play_family_planning_vid'].fillna('skipped')
bp_df['bp_vid_answered'] = (bp_df['form.bp2.play_birth_preparedness_vid'] != '---')
bp_df['fp_vid_answered'] = (bp_df['form.play_family_planning_vid'] != '---')
logging.info('% of BP forms that played Birth Prep Vid')
bp_bp_play_count = bp_df['form.bp2.play_birth_preparedness_vid'].value_counts(dropna=False)
logging.info(bp_bp_play_count * 100. / num_bp_forms)
logging.info('% of BP forms that played Family Planning Vid')
bp_fp_play_count = bp_df['form.play_family_planning_vid'].value_counts(dropna=False)
logging.info(bp_fp_play_count * 100. / num_bp_forms)

bp_bp_vid_xtab = pd.crosstab(bp_df['trimester'],bp_df['form.bp2.play_birth_preparedness_vid'], dropna=False)
bp_bp_vid_xtab['total'] = bp_bp_vid_xtab['yes'] + bp_bp_vid_xtab['no'] + bp_bp_vid_xtab['skipped']
bp_bp_vid_xtab['no%'] = bp_bp_vid_xtab['no'] * 100. / bp_bp_vid_xtab['total']
bp_bp_vid_xtab['yes%'] = bp_bp_vid_xtab['yes'] * 100. / bp_bp_vid_xtab['total']
bp_bp_vid_xtab['skip%'] = bp_bp_vid_xtab['skipped'] * 100. / bp_bp_vid_xtab['total']
logging.info(bp_bp_vid_xtab)
bp_fp_vid_xtab = pd.crosstab(bp_df['trimester'],bp_df['form.play_family_planning_vid'], dropna=False)
bp_fp_vid_xtab['total'] = bp_fp_vid_xtab['yes'] + bp_fp_vid_xtab['no'] + bp_fp_vid_xtab['skipped']

logging.info('BP form durations:')
logging.info('All BP forms.  Mean: %0.1f Median: %0.1f' % (bp_df['form_duration'].mean(), bp_df['form_duration'].median()))
logging.info('All 3rd Trimester BP forms.  Mean: %0.1f Median: %0.1f' % (bp_df[(bp_df['trimester'] == 'third')]['form_duration'].mean(), bp_df[(bp_df['trimester'] == 'third')]['form_duration'].median()))
logging.info('BP forms w/ watch BP = Yes.  Mean: %0.1f Median: %0.1f' % (bp_df[(bp_df['form.bp2.play_birth_preparedness_vid'] == 'yes')]['form_duration'].mean(), bp_df[(bp_df['form.bp2.play_birth_preparedness_vid'] == 'yes')]['form_duration'].median()))
logging.info('BP forms w/ watch BP != Yes  Mean: %0.1f Median: %0.1f' % (bp_df[(bp_df['form.bp2.play_birth_preparedness_vid'] == 'no') | (bp_df['form.bp2.play_birth_preparedness_vid'] == 'skipped')]['form_duration'].mean(), bp_df[(bp_df['form.bp2.play_birth_preparedness_vid'] == 'no') | (bp_df['form.bp2.play_birth_preparedness_vid'] == 'skipped')]['form_duration'].median()))
logging.info('BP forms w/ watch FP = Yes.  Mean: %0.1f Median: %0.1f' % (bp_df[(bp_df['form.play_family_planning_vid'] == 'yes')]['form_duration'].mean(), bp_df[(bp_df['form.play_family_planning_vid'] == 'yes')]['form_duration'].median()))
logging.info('BP forms w/ watch FP != Yes  Mean: %0.1f Median: %0.1f' % (bp_df[(bp_df['form.play_family_planning_vid'] == 'no') | (bp_df['form.play_family_planning_vid'] == 'skipped')]['form_duration'].mean(), bp_df[(bp_df['form.play_family_planning_vid'] == 'no') | (bp_df['form.play_family_planning_vid'] == 'skipped')]['form_duration'].median()))


logging.info(bp_fp_vid_xtab)
bp_third = bp_df[bp_df['trimester'] == 'third']
bp_bp_users = pd.crosstab(bp_third['username'],bp_third['form.bp2.play_birth_preparedness_vid']).reset_index()
bp_bp_users['total'] = bp_bp_users['yes'] + bp_bp_users['no'] + bp_bp_users['skipped']
bp_bp_users['no%'] = bp_bp_users['no'] * 100. / bp_bp_users['total']
bp_bp_users['yes%'] = bp_bp_users['yes'] * 100. / bp_bp_users['total']
bp_bp_users['skip%'] = bp_bp_users['skipped'] * 100. / bp_bp_users['total']
axes = bp_bp_users.hist(column=['yes%'])
plt.title('Birth Preparedness Video in BP form')
y = axes[0, 0].set_ylabel(ylabel='Count of Users', labelpad=None)
x = axes[0, 0].set_xlabel(xlabel='Pct of Time User Answers Yes', labelpad=None)
logging.info('User behavior for BP vids in BP form:')
logging.info('Pct of users answering No over 90pct of time: %0.1f' % ((bp_bp_users['no%'] >= 90).sum() * 100. / (bp_bp_users['total'] > 0).sum()))
logging.info('Pct of users answering Yes over 90pct of time: %0.1f' % ((bp_bp_users['yes%'] >= 90).sum() * 100. / (bp_bp_users['total'] > 0).sum()))
logging.info('Pct of users answering Skip over 90pct of time: %0.1f' % ((bp_bp_users['skip%'] >= 90).sum() * 100. / (bp_bp_users['total'] > 0).sum()))

bp_fp_users = pd.crosstab(bp_third['username'],bp_third['form.play_family_planning_vid']).reset_index()
bp_fp_users['total'] = bp_fp_users['yes'] + bp_fp_users['no'] + bp_fp_users['skipped']
bp_fp_users['no%'] = bp_fp_users['no'] * 100. / bp_fp_users['total']
bp_fp_users['yes%'] = bp_fp_users['yes'] * 100. / bp_fp_users['total']
bp_fp_users['skip%'] = bp_fp_users['skipped'] * 100. / bp_fp_users['total']
axes = bp_fp_users.hist(column=['yes%'])
plt.title('Family Planning Video in BP form')
y = axes[0, 0].set_ylabel(ylabel='Count of Users', labelpad=None)
x = axes[0, 0].set_xlabel(xlabel='Pct of Time User Answers Yes', labelpad=None)
logging.info('User behavior for FP vids in BP form:')
logging.info('Pct of users answering No over 90pct of time: %0.1f' % ((bp_fp_users['no%'] >= 90).sum() * 100. / (bp_fp_users['total'] > 0).sum()))
logging.info('Pct of users answering Yes over 90pct of time: %0.1f' % ((bp_fp_users['yes%'] >= 90).sum() * 100. / (bp_fp_users['total'] > 0).sum()))
logging.info('Pct of users answering Skip over 90pct of time: %0.1f' % ((bp_fp_users['skip%'] >= 90).sum() * 100. / (bp_fp_users['total'] > 0).sum()))


# CF form info
cf_dir = os.path.join(target_dir, '[DA] Complementary Feeding - min_video')
cf_df = gf.csv_files_to_df(cf_dir, data_regex, date_cols = ['completed_time'])
logging.info('raw CF forms: %i' % cf_df.shape[0])
cf_df = gf.add_locations_by_username(cf_df)
logging.info('raw CF forms after add locations: %i' % cf_df.shape[0])
cf_df = cf_df.loc[(cf_df['state_name'].isin(real_state_list))]
num_cf_forms = cf_df.shape[0]
logging.info('Num Comp Feeding forms in real locations: %i' % num_cf_forms)
logging.info('%i different users submitted this form' % cf_df['awc_name'].nunique())
logging.info('%.2f average forms per user' % cf_df['awc_name'].value_counts().mean())

cf_df['form.add'] = pd.to_datetime(cf_df['form.add'])
cf_df['completed_time'] = pd.to_datetime(cf_df['completed_time'])
cf_df['started_time'] = pd.to_datetime(cf_df['started_time'])
cf_df['form_duration'] = (cf_df['completed_time'] - cf_df['started_time']) / np.timedelta64(1, 'm')
cf_df['days_age'] = (cf_df['completed_time'] - cf_df['form.add']) / np.timedelta64(1, 'D')

bins = [-9*30.4375, 6*30.4375, 9*30.4375, 12*30.4375 , 15*30.4375, 18*30.4375, 21*30.4375, 24*30.4375, 100*30.4375]
bin_names = ['< 6mths', '6-9mths','9-12mths', '12-15mths', '15-18mths', '18-21mths', '21-24mths', '24mths+']
cf_df['age_bin'] = pd.cut(cf_df['days_age'], bins, labels=bin_names)
logging.info('% Distribution of CF forms by age:')
logging.info(cf_df['age_bin'].value_counts(dropna=False) * 100. / num_cf_forms)
cf_df['group'] = bp_df['days_to_edd'].apply(lambda x: 'third' if x <= 91 else ('second' if x <= 182 else 'first'))
cf_df['form.play_comp_feeding_vid'] = cf_df['form.play_comp_feeding_vid'].fillna('skipped')
cf_play_count = cf_df['form.play_comp_feeding_vid'].value_counts(dropna=False)
logging.info(cf_play_count * 100. / num_cf_forms)
cf_vid_xtab = pd.crosstab(cf_df['age_bin'],cf_df['form.play_comp_feeding_vid'], dropna=False)
cf_vid_xtab['total'] = cf_vid_xtab.sum(axis=1)
cf_vid_xtab['no%'] = cf_vid_xtab['no'] * 100. / cf_vid_xtab['total']
cf_vid_xtab['yes%'] = cf_vid_xtab['yes'] * 100. / cf_vid_xtab['total']
logging.info(cf_vid_xtab)

logging.info('CF form durations:')
logging.info('All CF forms.  Mean: %0.1f Median: %0.1f' % (cf_df['form_duration'].mean(), cf_df['form_duration'].median()))
logging.info('CF forms w/ watch = Yes.  Mean: %0.1f Median: %0.1f' % (cf_df[(cf_df['form.play_comp_feeding_vid'] == 'yes')]['form_duration'].mean(), cf_df[(cf_df['form.play_comp_feeding_vid'] == 'yes')]['form_duration'].median()))
logging.info('CF forms w/ watch != Yes  Mean: %0.1f Median: %0.1f' % (cf_df[(cf_df['form.play_comp_feeding_vid'] == 'no') | (cf_df['form.play_comp_feeding_vid'] == 'skipped')]['form_duration'].mean(), cf_df[(cf_df['form.play_comp_feeding_vid'] == 'no') | (cf_df['form.play_comp_feeding_vid'] == 'skipped')]['form_duration'].median()))


# Video Library forms
vid_dir = os.path.join(target_dir, '[DA] Video Library')
vid_df = gf.csv_files_to_df(vid_dir, data_regex, vid_date_cols)
logging.info('raw vid library forms: %i' % vid_df.shape[0])
vid_df = gf.add_locations_by_username(vid_df)
logging.info('raw vid libary forms after add locations: %i' % vid_df.shape[0])
vid_df = vid_df.loc[(vid_df['state_name'].isin(real_state_list))]
num_vid_forms = vid_df.shape[0]
logging.info('Num Video Library forms in real locations: %i' % num_vid_forms)
logging.info('%i different users submitted this form' % vid_df['awc_name'].nunique())
logging.info('%.2f average forms per user' % vid_df['awc_name'].value_counts().mean())

# look at video form itself
vid_df['form_duration'] = (vid_df['completed_time'] - vid_df['started_time']) / np.timedelta64(1, 'm')
logging.info(vid_df['form_duration'].describe())
vid_bins = [-10000, 0, 1, 5, 10,15,20,25,30,60,100000]
vid_bin_labels = ['<0','0-1min','1-5min','5-10min','10-15min','15-20min','20-25min','25-30min','30-60min','60min+']
vid_df['duration_bin'] = pd.cut(vid_df['form_duration'], bins=vid_bins, labels=vid_bin_labels)
logging.info('Duration form open in minutes')
logging.info(vid_df['duration_bin'].value_counts() * 100. / num_vid_forms)
logging.info('Duration options for type of video to watch')
logging.info(vid_df['form.video_choice'].value_counts(dropna=False) * 100. / num_vid_forms)

vid_count = pd.DataFrame()
for video in video_list:
    temp = vid_df[video].value_counts(dropna=False)
    vid_count = vid_count.append(temp)
total_views = vid_count['yes'].sum()
vid_count['view%'] = vid_count['yes'] *100. / total_views
logging.info('Video Play by % of all videos chosen to be shown')
logging.info(vid_count['view%'])

logging.info('Birth Prep video: played %i times in BP HV form, played %i times in Video Library' % (bp_bp_play_count['yes'], vid_count.loc['form.counseling.bp_videos']['yes']))
logging.info('Family Planning video: played %i times in BP HV form, played %i times in Video Library' % (bp_fp_play_count['yes'], vid_count.loc['form.counseling.is_family_planning']['yes']))
logging.info('Comp Feeding videos: played %i times in CF HV form, played %i times in Video Library' % (cf_play_count['yes'], vid_count.loc['form.counseling.complementary_feeding']['yes']))



vid_users = pd.DataFrame()
vid_users['count'] = vid_df.groupby(['username'])['form_duration'].count()
vid_users['mean'] = vid_df.groupby(['username'])['form_duration'].mean()
vid_users['median'] = vid_df.groupby(['username'])['form_duration'].median()
vid_users['std'] = vid_df.groupby(['username'])['form_duration'].std()
vid_users['max'] = vid_df.groupby(['username'])['form_duration'].max()
vid_users['min'] = vid_df.groupby(['username'])['form_duration'].min()
logging.info('Removing %i users who had mean video play sessions over 30 minutes, out of %i users' % ((vid_users['mean'] > 60).sum(), vid_users.shape[0]))
logging.info('and %i users who have submitted more than 50 video forms' % (vid_users['count'] > 25).sum())
vid_users_trimmed = vid_users[(vid_users['mean'] <= 30) & (vid_users['count'] <= 25)]
axes = vid_users_trimmed.hist(column=['count'], bins=25)
plt.title('Video Library Form Usage - Form Count')
plt.xlim([0, 25])
y = axes[0, 0].set_ylabel(ylabel='Count of Users', labelpad=None)
x = axes[0, 0].set_xlabel(xlabel='Number of Forms per User', labelpad=None)

axes = vid_users_trimmed.hist(column=['mean'], bins=25)
plt.title('Video Library Form Usage - Mean Duration')
plt.xlim([0, 30])
y = axes[0, 0].set_ylabel(ylabel='Count of Users', labelpad=None)
x = axes[0, 0].set_xlabel(xlabel='Mean Form Duration (min)', labelpad=None)
