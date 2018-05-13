# -*- coding: utf-8 -*-
"""
Functions for Case Property Analysis

these functions support ICDS case property analysis

@author: Matt Theis, July 2017
"""
import logging
import numpy as np
import pandas as pd
import gen_func
import datetime


def clean_case_data(df, output_dict, awc_test=True):
    '''
    Clean case data if columns to do so exist in the data files.

    Can remove from df: closed cases, cases not in a real state,
    cases belonging to a user with 'test' in the username

    Informational: # of person cases without an associated household parent id

    Parameters
    ----------
    df : pandas dataframe
      Dataframe of case data to clean

    output_dict : dictionary
      Output file to append key/value data/value pairs

    Returns
    -------
    df : pandas dataframe
      Cleaned dataframe of case data

    output_dict : dictionary
      Updated output file data/value pairs describing cleaning

    '''
    logging.info('Cleaning case data...')
    orig_rows = len(df.index)
    logging.info('%i rows found' % orig_rows)

    # get rid of any closed cases by looking for 'closed'==True
    if 'closed' in df.columns.tolist():
        num_closed = df.groupby('closed').size()[True]
        logging.info('%i closed removed' % num_closed)
        df = df[df['closed'] != True]
    else:
        logging.info('Not removing closed cases, closed column not found.')
        num_closed = np.nan

    # due to the way data is collected, indices.household isn't a good detector
    # of orphan cases.  need to revisit after talking more with simon
    '''
    # cases wo associated household parent case ids, 'indices.household' isnull
    # DO NOT remove from dataframe - return number as informational only
    if 'indices.household' in df.columns.tolist():
        num_wo_hh_id = df[df['indices.household'].isnull()].shape[0]
        logging.info('%i with no household parent cases (not removed from df)'
                     % num_wo_hh_id)
        # df = df[df['indices.household'].notnull()]
    else:
        logging.info('Not analyzing blank parent household ids, '
                     'indices.household column not found.')
        num_wo_hh_id = np.nan
    '''

    # cases with a blank name
    if 'name' in df.columns.tolist():
        orig_df_len = len(df.index)
        df = value_is_blank(df, 'name')
        num_all_numeric, all_num_df, all_num_index = string_contains(
                df, 'name', '^\d+$', 'only numeric chars in name', index_out=True)
        df = df[~all_num_index]
        num_numeric, some_num_df, some_num_index = string_contains(
                df, 'name', '\d+', 'some numeric chars in name', index_out=True)
        # df = df[~some_num_index]
        logging.info('Not removing names with at least one numeric character')
        num_blank_name = orig_df_len - len(df.index)

        if num_all_numeric != 0 or num_numeric != 0:
            output_frames = []
            output_frames.append(all_num_df)
            output_frames.append(some_num_df)
            error_df = pd.concat(output_frames, ignore_index=True, copy=False)
            name_err_filename = 'numeric_case_names_' + str(datetime.date.today()) + '.csv'
            logging.info('Creating csv output of names with numbers at %s'
                         % name_err_filename)
            error_df.to_csv(name_err_filename, encoding='utf-8')
    else:
        logging.info('Not analyzing for blank names, name column not found.')
        num_blank_name = np.nan

    # test state locations or that aren't an AWC
    # add temp location columns, filter out, then remove location columns
    if 'owner_id' in df.columns.tolist():
        pre_test_loc_total = len(df.index)
        location_columns = ['doc_id', 'state_name', 'awc_name']
        df = gen_func.add_locations(df, 'owner_id', location_columns)
        real_state_list = ['Uttar Pradesh', 'Madhya Pradesh', 'Chhattisgarh',
                           'Andhra Pradesh', 'Bihar', 'Jharkhand', 'Rajasthan',
                           'Maharashtra']
        logging.debug('Cases by state:')
        logging.debug(df['state_name'].value_counts(dropna=False))
        df = df.loc[(df['state_name'].isin(real_state_list))]
        num_test_locations = pre_test_loc_total - len(df.index)
        df.drop(['state_name'], 1)
        logging.info('%i cases in test locations or that are not AWCs removed'
                     % num_test_locations)
        # if find non test locations, lets send that to an csv to inspect
        if num_test_locations != 0:
            non_realstate_df = df.loc[~df['state_name'].isin(real_state_list)]
            filename = 'non_realstate_case_' + str(datetime.date.today()) + '.csv'
            logging.info('Creating csv output of presumed test locations at %s'
                         % filename)
            non_realstate_df.to_csv(filename)
    else:
        logging.info('Not removing test or non-AWC locations, owner_id column '
                     'not found.')
        num_test_locations = np.nan

    # looks for string 'test' in any username
    if 'username' in df.columns.tolist():
        num_test_users = df[df['username'].str.contains('test')].shape[0]
        logging.info('%i with _test_ in username removed' % num_test_users)
        df = df[~df['username'].str.contains('test')]
    elif 'awc_name' in df.columns.tolist():
        num_test_users = df[df['awc_name'].str.contains('test')].shape[0]
        logging.info('%i with _test_ in awc_name removed' % num_test_users)
        df = df[~df['awc_name'].str.contains('test')]
    elif 'owner_name' in df.columns.tolist():
        num_test_users = df[df['owner_name'].str.contains('test')].shape[0]
        logging.info('%i with _test_ in owner_name removed' % num_test_users)
        df = df[~df['owner_name'].str.contains('test')]
    else:
        logging.info('Not removing usernames for test users, neither username '
                     'nor awc_name nor owner_name column found.')
        num_test_users = np.nan

    # look for locations for the case that aren't associated with awcs
    if (('commcare_location_id' in df.columns.tolist() or
            'owner_id' in df.columns.tolist()) and awc_test is True):
        non_awc_num, non_awc_df, non_awc_index = search_non_awc_owners(df, index_out=True)
        df = df[~non_awc_index]
    else:
        logging.info('Not removing non-awc locations, neither owner_id nor '
                     'commcare_location_id found.')
        non_awc_num = np.nan

    # prepare outputs
    num_clean_rows = len(df.index)
    logging.info('Returning dataframe with %i rows' % num_clean_rows)
    output_dict.update({'orig_rows': orig_rows,
                        # 'num_wo_hh_id': num_wo_hh_id,
                        'num_blank_name': num_blank_name,
                        'non_awc_num': non_awc_num,
                        'num_closed': num_closed,
                        'num_test_locations': num_test_locations,
                        'num_test_users': num_test_users,
                        'num_clean_rows': num_clean_rows})
    return df, output_dict


def string_starts_with(df, column, start_char, test_name, index_out=False):
    '''
    Test a column in a dataframe for a specific starting character
    
    Parameters
    ----------
    df : pandas dataframe
      Input dataframe
    column : string
      Name of column in dataframe to test
    start_char : string or regex
      String to look for in each value in the column
    test_name : string
      Add a column to the output that contains this tag
    index_out : boolean
      (Optional) Include a boolean series on the original df.  False is default

    Returns
    -------
    num_with_string : int
      Number of values in dataframe that contain input string
    has_string_df : pandas dataframe
      Dataframe that only contains specified strings in tested column
    has_string_index : pandas series of booleans
      Boolean index on the orig df of values containing strings (only included
      if index_out=True)
    '''
    df[column] = df[column].astype(str)
    does_start_index = df[column].str.startswith(start_char)
    does_start_df = df[does_start_index]
    num_with_start = len(does_start_df.index)
    does_start_df['error'] = test_name
    logging.info('Found total of %i in dataset of %i for test: %s'
                 % (num_with_start, len(df.index), test_name))
    if not index_out:
        return num_with_start, does_start_df
    else:
        return num_with_start, does_start_df, does_start_index
   

def string_contains(df, column, string, test_name, index_out=False):
    '''
    Test a column in a dataframe for presence of a substring
    using str.contains(string), ie - see if a string contains a sequence '123'
    By default, only return number and dataframe, not index of original df

    Parameters
    ----------
    df : pandas dataframe
      Input dataframe
    column : string
      Name of column in dataframe to test
    string : string or regex
      String to look for in each value in the column
    test_name : string
      Add a column to the output that contains this tag
    index_out : boolean
      (Optional) Include a boolean series on the original df.  False is default

    Returns
    -------
    num_with_string : int
      Number of values in dataframe that contain input string
    has_string_df : pandas dataframe
      Dataframe that only contains specified strings in tested column
    has_string_index : pandas series of booleans
      Boolean index on the orig df of values containing strings (only included
      if index_out=True)
    '''
    df[column] = df[column].astype(str)
    has_string_index = df[column].str.contains(string)
    has_string_df = df[has_string_index]
    num_with_string = len(has_string_df.index)
    has_string_df['error'] = test_name
    logging.info('Found total of %i in dataset of %i for test: %s'
                 % (num_with_string, len(df.index), test_name))
    if not index_out:
        return num_with_string, has_string_df
    else:
        return num_with_string, has_string_df, has_string_index


def string_not_contains(df, column, string, test_name, index_out=False):
    '''
    Test a column in a dataframe for absence of a substring
    using ~str.contains(string), opposite of string_contains
    By default, only return number and dataframe, not index of original df

    Parameters
    ----------
    df : pandas dataframe
      Input dataframe
    column : string
      Name of column in dataframe to test
    string : string or regex
      String to ensure is NOT included in each value in the column
    test_name : string
      Add a column to the output that contains this tag
    index_out : boolean
      (Optional) Include a boolean series on the original df.  False is default

    Returns
    -------
    num_no_string : int
      Number of values in dataframe that no not contain input string
    no_string_df : pandas dataframe
      Dataframe that only has values that don't include specified string
    has_string_index : pandas series of booleans
      Boolean index on the orig df of values not containing strings (only
      included if index_out=True)
    '''
    df[column] = df[column].astype(str)
    no_string_index = ~df[column].str.contains(string)
    no_string_df = df.loc[no_string_index]
    num_no_string = len(no_string_df.index)
    no_string_df['error'] = test_name
    logging.info('Found total of %i in dataset of %i for test: %s'
                 % (num_no_string, len(df.index), test_name))
    if not index_out:
        return num_no_string, no_string_df
    else:
        return num_no_string, no_string_df, no_string_index


def string_length(df, column, desired_length, test_name, index_out=False):
    '''
    Test a column in a dataframe for length of a specific string
    ie: see if a string value is 12 digits.
    By default, only return number and dataframe, not index of original df
    '''
    bad_length_index = df[column].str.len() != desired_length
    bad_length_df = df.loc[bad_length_index]
    num_bad_length = len(bad_length_df.index)
    bad_length_df['error'] = test_name
    logging.info('Found total of %i in dataset of %i for test: %s'
                 % (num_bad_length, len(df.index), test_name))
    if not index_out:
        return num_bad_length, bad_length_df
    else:
        return num_bad_length, bad_length_df, bad_length_index


def string_not_null(df, column, test_name):
    '''
    Test a column in a dataframe for null values.  Uses .isnull()
    '''
    error_df = df[df[column].isnull()]
    non_error_df = df[df[column].notnull()]
    num_errors = len(error_df.index)
    error_df.loc[: ('error')] = test_name
    logging.info('Found total of %i null values in dataset of %i for test: %s'
                 % (num_errors, len(df.index), test_name))
    return num_errors, error_df, non_error_df


def add_age_info(df, col_name='age_bracket', bin_type='brackets', relative_date='today'):
    '''
    Add age information to a dataframe.

    Parameters
    ----------
    df : pandas dataframe
      Dataframe of case data to clean

    col_name : string
      Title of new column to add age info (Optional, defaults to 'age_bracket')

    bin_type : string
      Type of bin.  Brackets vs yearly.  (Optional, defaults to 'brackets')

    relative_date : string
      Name of date column in df to calculate age (Optional, defaults to today)

    Returns
    -------
    df : pandas dataframe
      Dataframe with 'age_bracket' column broken down by buckets. Default buckets:
      ['0-5 yrs', '5-6 yrs', '6-15 yrs', '15-49 yrs', '49-99 yrs', '99 yrs+']

    '''
    if 'dob' in df.columns.tolist():
        if relative_date == 'today':
            df['age_days'] = (datetime.date.today() - pd.to_datetime(
                    df['dob'], errors='coerce')) / np.timedelta64(1, 'D')
        else:
            df['age_days'] = (df[relative_date] - pd.to_datetime(
                    df['dob'], errors='coerce')) / np.timedelta64(1, 'D')
        if bin_type == 'yearly':
            bins = [-100*365.25, 0*365.25, 1*356.25, 2*365.25, 3*365.25, 4*365.25, 5*365.25,
                    6*365.25, 7*356.25, 8*365.25, 9*365.25, 10*365.25,
                    11*365.25, 12*356.25, 13*365.25, 14*365.25, 15*365.25,
                    16*365.25, 49*365.25, 99*365.25, 500*365.25]
            bin_names = ['<0 yrs', '0-1 yrs', '1-2 yrs', '2-3 yrs', '3-4 yrs', '4-5 yrs',
                         '5-6 yrs', '6-7 yrs', '7-8 yrs', '8-9 yrs', '9-10 yrs',
                         '10-11 yrs', '11-12 yrs', '12-13 yrs', '13-14 yrs',
                         '14-15 yrs', '15-16 yrs', '16-49 yrs', '49-99 yrs',
                         '99 yrs+']
        else: # defaults to standard buckets
            bins = [-100*365.25, 0, 5*365.25, 11*365.25, 14*365.25, 49*365.25, 99*365.25, 1000*365.25]
            bin_names = ['<0 yrs', '0-5 yrs', '5-11 yrs', '11-14 yrs', '14-49 yrs', '49-99 yrs', '99 yrs+']
            #bins = [-100*365.25, 0, 5*365.25, 15*365.25, 49*365.25, 99*365.25, 1000*365.25]
            #bin_names = ['<0 yrs', '0-5 yrs', '5-15 yrs', '15-49 yrs', '49-99 yrs', '99 yrs+']
        df[col_name] = pd.cut(df['age_days'], bins, labels=bin_names)
    else:
        logging.info('ERROR - could not find dob in columns and unable to \
                     add age information to dataframe')
    return df


def check_beneficiary(df, relative_date='today'):
    '''
    Adds two T/F columns to your DF indicating if the person case is an ICDS
    beneficiary or not (either Female 15-49 or 0-5)
    '''
    # get age
    if 'dob' in df.columns.tolist():
        if relative_date == 'today':
            df['age_days'] = (datetime.date.today() - pd.to_datetime(
                    df['dob'], errors='coerce')) / np.timedelta64(1, 'D')
        else:
            df['age_days'] = (df[relative_date] - pd.to_datetime(
                    df['dob'], errors='coerce')) / np.timedelta64(1, 'D')
        df['child_ben'] = (df['age_days'] >= 0) & (df['age_days'] <= 365.25*5)
    else:
        logging.info('ERROR - could not find dob in columns and unable to \
                     add age information to dataframe')
    # see if Female 15-49
    if 'sex' and 'dob' in df.columns.tolist():
        df['female_ben'] = (df['sex']=='F') & (df['age_days'] >= 365.25*15) & (df['age_days'] <= 365.25*49)
    else:
        logging.info('ERROR - could not find sex in columns and unable to add \
                     female beneficiary information')
    df = df.drop('age_days', axis=1)
    return df


def apply_regex(regex_to_test, string_to_test):
    '''apply a regex to a string, return True for a match, False if no match'''
    mo = regex_to_test.search(string_to_test)
    if mo:
        return True
    else:
        return False


def num_in_age_bracket(df, age_limit='99 yrs+', index_out=False):
    '''
    Test a dataframe for existence of specific age bracket.  Uses 'age_bracket'
    column added in add_age_info().  Age to test defaults to '99 yrs+'.
    '''
    if 'age_bracket' in df.columns.tolist():
        error_df_index = df['age_bracket'] == age_limit
        error_df = df[error_df_index]
        num_errors = len(error_df.index)
        error_df['error'] = 'bad_age_bracket'
        logging.info('Found total of %i cases in dataset of %i in age '
                     'bracket: %s' % (num_errors, len(df.index), age_limit))
        if not index_out:
            return num_errors, error_df
        else:
            return num_errors, error_df, error_df_index
    else:
        logging.info('No age_bracket column found.  Run add_age_info. Not '
                     'removing based on age.')
        if not index_out:
            return 0, df
        else:
            return 0, df, df.index


def search_non_awc_owners(df, index_out=False):
    '''
    Find cases that aren't owned by an AWC by referencing either owner_id
    or commcare_location_id with the location fixture.
    '''
    if 'commcare_location_id' in df.columns.tolist():
        case_df = gen_func.add_usertype_from_id(df, 'commcare_location_id')
    elif 'owner_id' in df.columns.tolist():
        case_df = gen_func.add_usertype_from_id(df, 'owner_id')
    else:
        logging.info('ERROR - no location column to use to find owner type')
        return

    # look for cases that aren't owned by an awc if have 'location_type' column
    non_awc_owner_index = df['location_type'] != 'aww'
    non_awc_owner_df = df.loc[non_awc_owner_index]
    num_non_awc_owner_df = len(non_awc_owner_df.index)
    non_awc_owner_df['error'] = 'owner_not_awc'
    logging.info('Found %s cases owned by non-aww' % num_non_awc_owner_df)
    if index_out:
        return num_non_awc_owner_df, non_awc_owner_df, non_awc_owner_index
    else:
        return num_non_awc_owner_df, non_awc_owner_df


def is_english(s):
    '''
    Pass in a string.  Return boolean if can be encoded in ascii, which acts
    as a proxy for being western characters.
    hat tip: https://stackoverflow.com/questions/27084617/detect-strings-with-non-english-characters-in-python
    '''
    try:
        s.encode(encoding='utf-8').decode('ascii')
    except UnicodeDecodeError:
        return False
    else:
        return True


def test_for_english(df, column):
    '''
    Test to see if a value in a dataframe contains only english chars

    Parameters
    ----------
    df : pandas dataframe
      Dataframe to analyze
    column : string
      Column in dataframe to analyze

    Returns
    -------
    num_english_only : int
      Number of values in df[column] that are only western chars
    english_only_df : pandas dataframe
      Dataframe of western only chars for given column
    english_only_index : pandas boolean series
      Index column on original dataframe with boolean for containing western
      only chars
    '''
    english_only_index = df[column].apply(is_english)
    english_only_df = df[english_only_index]
    num_english_only = len(english_only_df.index)
    logging.info('Found %s cases that have english names' % num_english_only)
    return num_english_only, english_only_df, english_only_index


def value_is_blank(df, column_name):
    '''
    Looks for blank and skipped values in a column in a dataframe

    Parameters
    ----------
    df : pandas dataframe
      Dataframe of values
    column_name : string
      Name of column in dataframe to analyze for blanks/skips

    Returns
    -------
    df : pandas dataframe
      Dataframe of values with no blanks or skips
    '''
    if column_name in df.columns.tolist():
        logging.info('Looking through %i rows' % len(df.index))
        # look for blanks first
        blank_index = df[column_name].isnull()
        # avoid error if no True values
        if True in blank_index.value_counts():
            num_blank = blank_index.value_counts()[True]
        else:
            num_blank = 0
        df = df[~blank_index]
        logging.info('%i cells with blank %s value found (%i pct of %i)'
                     % (num_blank, column_name,
                        100 * num_blank / len(df.index), len(df.index)))
        # then look for skipped values
        skipped_index = df[column_name] == '---'
        # avoid error if no True values
        if True in skipped_index.value_counts():
            num_skipped = skipped_index.value_counts()[True]
        else:
            num_skipped = 0
        df = df[~skipped_index]
        logging.info('%i additional cells with skipped \'---\' %s value found '
                     '(%i pct of %i)'
                     % (num_skipped, column_name,
                        100 * num_skipped / len(df.index), len(df.index)))
        # then look for values that are 'nan'
        nan_index = df[column_name] == 'nan'
        # avoid error if no True values
        if True in nan_index.value_counts():
            num_nan = nan_index.value_counts()[True]
        else:
            num_nan = 0
        df = df[~nan_index]
        logging.info('%i additional cells with skipped \'nan\' %s value found '
                     '(%i pct of %i)'
                     % (num_nan, column_name,
                        100 * num_nan / len(df.index), len(df.index)))
        logging.info('Removed %i blank/nan/null values'
                     % (num_blank + num_skipped + num_nan))
        logging.info('Returning dataframe with %i rows' % len(df.index))
    else:
        logging.info('Column name not found.  Not removing blanks or skips.')
    return df


def compare_values(df, column_name1, column_name2):
    '''
    Compare values of two columns in a dataframe for each row

    Parameters
    ----------
    df : pandas dataframe
      Dataframe of values
    column_name1 : string
      Name of first column in dataframe to compare
    column_name2 : string
      Name of second column in dataframe to compare

    Returns
    -------
    df_same_index : pandas index
      Index with True as column_name1 == column_name2 in the df.  Same length
      as input dataframe.
    num_same : int
      Number of values that are the same
    '''
    df_same_index = df[column_name1] == df[column_name2]
    # avoid error if no True values
    if True in df_same_index.value_counts():
        num_same = df_same_index.value_counts()[True]
    else:
        num_same = 0
    logging.info('%s is the same as %s in %i cases (%i pct of %i)'
                 % (column_name1, column_name2, num_same,
                    100 * num_same / len(df.index), len(df.index)))
    return df_same_index, num_same
