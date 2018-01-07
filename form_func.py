# -*- coding: utf-8 -*-
"""
Created on Mon Oct 16 14:47:38 2017

@author: theism
"""
import logging
import pandas as pd
import gen_func
import os
from matplotlib import pyplot as plt
import numpy as np
import matplotlib as mpl
import calendar


def download_form_ucr(start_date, end_date, cred_path, target_dir,
                      refresh_recent, refresh_days, data_regex):
    '''
    Downloads form submission UCR with option to refresh data directory

    Parameters
    ----------
    start_date : pd.Timestamp
      Beginning date for range of dates care about
    end_date : pd.Timestamp
      End date for range of dates care about.
    cred_path : string
      Absolute path to the credential file holding CommCare user/pwd
    target_dir : string
      Absolute path to directory with all the data
    refresh_recent : boolean
      True will delete recent files and redownload (to get dawdling form submissions)
    refresh_days : integer
      Number of days to refresh data
    data_regex : re object
      Regex on the type of file that contains data

    Returns
    -------
    file_list : list of strings
      List of files in the target directory
    '''
    # identify all the files to go through
    file_list = gen_func.data_file_list(target_dir, data_regex)

    # download files if see that any are missing
    logging.info('Checking if all files have been downloaded')
    all_dates = pd.date_range(start_date, end_date, freq='D')
    download_link = 'https://www.icds-cas.gov.in/a/icds-cas/configurable_reports/data_sources/export/static-icds-cas-static-usage_forms/?format=csv&form_date='
    user, password = gen_func.get_credentials(cred_path, 'icds')

    # get dates of files that have been downloaded
    file_dates = list(pd.to_datetime(x[5:-4]) for x in file_list)

    # if need to refresh data is True, get dates of files to delete
    if refresh_recent is True:
        remove_dates = sorted(file_dates)[-refresh_days:]
        for date_to_del in remove_dates:
            remove_name = 'icds.' + date_to_del.strftime('%m.%d.%Y') + '.csv'
            os.remove(os.path.join(target_dir, remove_name))
            logging.info('Refreshing data file: %s' % remove_name)
            file_dates.remove(date_to_del)

    for date_to_check in all_dates:
        if date_to_check in file_dates:
            logging.debug('have data for %s already' % date_to_check)
        else:
            logging.info('Downloading form data for %s' % date_to_check)
            date_to_get = date_to_check.strftime('%Y-%m-%d')
            full_dwnld_link = download_link + date_to_get
            new_file_name = 'icds.' + date_to_check.strftime('%m.%d.%Y') + '.csv'
            gen_func.download_ucr(full_dwnld_link, user, password,
                                  new_file_name, target_dir)

    # update file list
    file_list = gen_func.data_file_list(target_dir, data_regex)
    return file_list


def file_subset_by_date(start_date, end_date, target_dir, data_regex):
    '''
    Looks within a folder of files and returns a subset of files by date range

    Parameters
    ----------
    start_date : pd.Timestamp
      Beginning date for range of dates care about
    end_date : pd.Timestamp
      End date for range of dates care about.
    target_dir : string
      Absolute path to directory with all the data
    data_regex : re object
      Regex on the type of file that contains data

    Returns
    -------
    output_list : list of strings
      List of files in the target directory
    '''
    output_list = []
    # identify all the files to go through
    file_list = gen_func.data_file_list(target_dir, data_regex)
    date_range = pd.date_range(start_date, end_date, freq='D')
    # go through full list and get subset
    for file_to_check in file_list:
        file_date = file_to_check[5:-4]
        if file_date in date_range:
            output_list.append(file_to_check)
    return output_list


def divisor(suffix, divisor_col, df, multiplier=1):
    '''Add columns to the dataframe, dividing by another column and multiplying
    by a scalar. Hardcoded set of column labels for now.

    Parameters
    ----------
    suffix : string
      String to append to column name for new columns
    divisor_col : string
      Name of column to divide other columns by
    df : pandas dataframe
      Dataframe to use
    multiplier: float
      Number to multiply results by.  Can be 100 for pct, Defaults to 1

    Returns
    -------
    df : pandas dataframe
      Same dataframe with additional columns
    '''
    df['pse' + '_' + suffix] = (df['pse'] / df[divisor_col]) * multiplier
    df['gmp' + '_' + suffix] = (df['gmp'] / df[divisor_col]) * multiplier
    df['thr' + '_' + suffix] = (df['thr'] / df[divisor_col]) * multiplier
    df['home_visit' + '_' + suffix] = (df['home_visit'] / df[divisor_col]) * multiplier
    df['due_list' + '_' + suffix] = (df['due_list'] / df[divisor_col]) * multiplier
    df['hh_mng' + '_' + suffix] = (df['hh_mng'] / df[divisor_col]) * multiplier
    df['total' + '_' + suffix] = (df['total'] / df[divisor_col]) * multiplier
    return df


def subtract(subtract_dict, df):
    '''Subtract scalar values from df columns.  Dict includes column names to
    subtract and the value to subract from them'''
    for key, value in subtract_dict.iteritems():
        df['exp_diff' + '_' + key] = df[key] - value
    return df


def assign_active_types(prev_value, test_value, test_date, start_date):
    '''
    Takes form submissions table and assigns first level of values to it:
    start, pre and active_1.  Requires calling assign_inactive afterwards

    Parameters
    ----------
    prev_value : string
      Previous value in table.  Expects pre/start/active_1
    test_value : float
      Number of form submissions for that day
    test_date : datetime
      Date of the form submission value
    start_date : datetime
      Date this user started submitting forms

    Returns
    -------
    string
      Pre / Start / Active_1 or number of sequential zero submission days
    '''
    # haven't submitted data yet
    if test_date < start_date:
        return 'pre'
    # first day of data submissions
    elif test_date == start_date:
        return 'start'
    # potential active/inactive user
    else:
        # active user
        if int(test_value) != 0:
            return 'active_1'
        # didn't submit today, keep count of number of days of inactivity
        # first inactive day next to start/active_1 or no prev_value
        elif prev_value == 'start' or prev_value == 'active_1' or prev_value == '':
            return '1'
        else:
            return str(int(prev_value) + 1)


def assign_inactive(x):
    if x == 'start' or x == 'active_1' or x == 'pre':
        return x
    elif int(x) < 3:
        return 'active_1-3'
    elif int(x) < 7:
        return 'active_3-7'
    elif int(x) < 14:
        return 'active_7-14'
    elif int(x) < 28:
        return 'active_14-28'
    elif int(x) < 100:
        return 'inactive_28-90'
    else:
        return 'inactive_90+'


def state_plot_panel(df, state_name, start, stop, plot_cats, exp_df, label_list):
    '''Creates a defined panel of plots for each state
    NOTE - never quite finished this function - isn't pretty yet'''
    f, axarr = plt.subplots(3, 3, figsize=[16, 12])
    mpl.style.use('default')
    f.suptitle(state_name + ' Form Submission Analysis from ' + start.strftime('%Y-%m-%d') + ' to ' + stop.strftime('%Y-%m-%d'))
    # form submissions by day area plot
    df[df['state_name'] == state_name].plot(kind='area', stacked=True, y=plot_cats['form_pct'], ax=axarr[0, 0], legend=True)
    axarr[0, 0].set_title('Percent of Form Submissions by Day')
    # form submissions count line plot
    df[df['state_name'] == state_name].plot(kind='line', y=plot_cats['form_nums'], ax=axarr[0, 1], legend=True)
    axarr[0, 1].set_title('Number of Form Submissions by Day')
    # averaged form submissions
    df[df['state_name'] == state_name].plot(kind='line', style=':', ax=axarr[0, 2])
    df[df['state_name'] == state_name].resample('1W').mean().plot(kind='line', ax=axarr[0, 2])
    df[df['state_name'] == state_name].resample('1M').mean().plot(kind='line', ax=axarr[0, 2])
    axarr[0, 2].set_title('Daily, Weekly Mean, Monthly Mean Submissions')
    # home visits
    df[df['state_name'] == state_name].plot(kind='area', stacked=True, y=plot_cats['home_visits'], ax=axarr[1, 0], legend=True)
    axarr[1, 0].set_title('Home Visit Form Breakdown')
    # bp home visits
    df[df['state_name'] == state_name].plot(kind='area', stacked=True, y=plot_cats['bp_visits'], ax=axarr[1, 1], legend=True)
    axarr[1, 1].set_title('BP Form Breakdown')
    # due list by type
    df[df['state_name'] == state_name].plot(kind='area', stacked=True, y=plot_cats['due_list_visits'], ax=axarr[1, 2], legend=True)
    axarr[1, 2].set_title('Due List Form Breakdown')
    # number of forms submissions per week
    pd.pivot_table(df[df['state_name'] == state_name], index=['weekday'], aggfunc=np.average).reindex(list(calendar.day_name)).plot(kind='bar', stacked=True, y=plot_cats['form_nums'], ax=axarr[2, 1], legend=True)
    axarr[2, 1].set_title('Avrg Form Submissions by Day of Week')
    # expected vs actual form submissions
    exp_df.plot(kind='barh', y=['diff_per_mth'], ax=axarr[2, 2], legend=False)
    axarr[2, 2].set_title('Expected Monthly Form Submissions')
    axarr[2, 2].set_yticklabels(label_list[state_name])
    return


def plot_all_states(total_df, state_df, y_data_list, state_list, start, stop,
                    plot_kind, stacked_bool=False, title='', total_y_lim=None, state_y_lim=None):   
    '''Split up plots of a certain type by state, one plot for total'''
    f, axarr = plt.subplots(3, 2, figsize=[16, 12])
    mpl.style.use('default')
    count = 0
    for row in range(0, 3):
        for col in range(0, 2):
            if count < len(state_list):
                state_df[state_df['state_name'] == state_list[count]].plot(kind=plot_kind,
                                                                           stacked=stacked_bool,
                                                                           y=y_data_list,
                                                                           ax=axarr[row, col],
                                                                           legend=False)
                axarr[row, col].set_title(state_list[count])
                if total_y_lim is not None:
                    axarr[row, col].set_ylim(state_y_lim)
                count += 1
    mpl.style.use('default')
    # total_df.plot(kind=plot_kind, stacked=stacked_bool, y=y_data_list, ax=axarr[2, 1], legend=False)
    # axarr[2, 1].set_title('Total')
    # if state_y_lim is not None:
    #     axarr[2, 1].set_ylim(total_y_lim)
    f.suptitle(title + ' ' + start.strftime('%Y-%m-%d') + ' to ' + stop.strftime('%Y-%m-%d'))
    axarr[0, 1].legend(bbox_to_anchor=(1, 1), loc=2)
    x=axarr[0, 0].set_xticklabels(labels='', visible=False)
    x=axarr[0, 1].set_xticklabels(labels='', visible=False)
    x=axarr[1, 0].set_xticklabels(labels='', visible=False)
    x=axarr[1, 1].set_xticklabels(labels='', visible=False)
    y=axarr[0, 0].set_xlabel(xlabel='', labelpad=None)
    y=axarr[0, 1].set_xlabel(xlabel='', labelpad=None)
    y=axarr[1, 0].set_xlabel(xlabel='', labelpad=None)
    y=axarr[1, 1].set_xlabel(xlabel='', labelpad=None)
    # f.tight_layout()
    return
