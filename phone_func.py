# -*- coding: utf-8 -*-
"""
Created on Tue Aug 08 20:05:41 2017

@author: theism
"""
import logging
import numpy as np
import pandas as pd
from case_func import *


def clean_phone_data(df, output_dict):
    '''
    Clean phone data from person cases.

    Can remove from df: contact number verified, 

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
    logging.info('Cleaning phone data...')
    orig_phone_rows = len(df.index)
    logging.info('%i rows found' % orig_phone_rows)

    # non verified phone numbers, contact_phone_number_is_verified != 1
    if 'contact_phone_number_is_verified' in df.columns.tolist():
        num_verified = df.groupby(
                'contact_phone_number_is_verified').size()['1']
        num_unverified = len(df.index)-num_verified
        logging.info('%i non-verified numbers removed' % num_unverified)
        df = df[df['contact_phone_number_is_verified'] == '1']
    else:
        num_unverified = np.nan
        logging.info('Not removing non-verified phone numbers, '
                     'contact_phone_number_is_verified column not found.')

    # get rid of non-female phone numbers, sex != F
    if 'sex' in df.columns.tolist():
        num_female = df.groupby('sex').size()['F']
        num_non_female = len(df.index)-num_female
        logging.info('%i non-female removed' % num_non_female)
        df = df[df['sex'] == 'F']
    else:
        num_non_female = np.nan
        logging.info('Not removing females, sex column not found.')

    # get rid of cases outside of age range 14.5 to 49.5 years.
    # uses dob and opened_date for approximate age at time of registration
    if 'dob' in df.columns.tolist() and 'opened_date' in df.columns.tolist():
        # 15 in form, since comparing to opened_date, adding extra for error
        min_age = 365.25*14.5
        # 49 in form, since comparing to opened_date, adding extra for error
        max_age = 365.25*49.5
        df['age_delta'] = (df['opened_date'] - pd.to_datetime(
                df['dob'])) / np.timedelta64(1, 'D')
        df['age_index'] = df['age_delta'].apply(
                lambda x: (min_age <= x) & (x <= max_age))
        num_in_age = df.groupby('age_index').size()[True]
        num_out_of_age = len(df.index)-num_in_age
        logging.info('%i too young, too old removed' % num_out_of_age)
        df = df[df['age_index'] == True]

        # remove columns added to figure out age
        #df.drop(['age_delta', 'age_index'])
    else:
        num_out_of_age = np.nan
        logging.info('Not removing cases younger than 15yrs or older than '
                     '49 yrs, dob and/or opened_date columns not found.')

    # prepare outputs
    num_clean_phone_nums = len(df.index)
    logging.info('%i clean phone numbers (out of %i cases, %i percent)'
                 % (num_clean_phone_nums, orig_phone_rows,
                    int(num_clean_phone_nums * 100 / orig_phone_rows)))
    logging.info('Returning dataframe with %i rows' % num_clean_phone_nums)
    output_dict.update({'num_unverified': num_unverified,
                        'num_non_female': num_non_female,
                        'num_out_of_age': num_out_of_age,
                        'num_clean_phone_nums': num_clean_phone_nums})
    return df, output_dict

def only_valid_phone(df, column, split_error_df=False):
    '''
    Creates a dataframe that is only 'good' phone numbers.  Checks for
    non-numeric characters, 12 digit length, 91 prefix, only verified
    numbers, and removes duplicates.

    Parameters
    ----------
    df : pandas dataframe
      Dataframe to analyze

    column : string
      Name of column to analyze for phone goodness

    split_error_df : boolean
      Input argument to return one df with all errors, or separate dfs

    Returns
    -------
    good_df : pandas dataframe
      Subset of original dataframe that only contains 'good' aadhar numbers

    bad_df : pandas dataframe
      Single df with errors.  Based on entry param, can be split

    non_numeric_df : pandas dataframe
      (Optional) Dataframe of all entries including non-numeric characters

    non_12_char_df: pandas dataframe
      (Optional) Dataframe of all entries that aren't 12 digits in length

    bad_prefix_df : pandas dataframe
      (Optional) Dataframe of all entries that don't start with 91
    '''
    logging.info('Looking for only valid phone numbers...')
    # non verified phone numbers, contact_phone_number_is_verified != 1
    if 'contact_phone_number_is_verified' in df.columns.tolist():
        num_verified = df.groupby(
                'contact_phone_number_is_verified').size()['1']
        num_unverified = len(df.index)-num_verified
        logging.info('%i non-verified numbers removed' % num_unverified)
        good_df = df[df['contact_phone_number_is_verified'] == '1']
    else:
        logging.info('Not removing non-verified phone numbers, '
                     'contact_phone_number_is_verified column not found.')

    # see if any character is NOT 0-9 in number
    num_non_numeric, non_numeric_df, non_numeric_index = string_contains(
            good_df, column, '\D+', 'non_numeric_char', index_out=True)
    good_df = good_df.loc[~non_numeric_index]

    # check string length
    num_non_12_char, non_12_char_df, non_12_char_index = string_length(
            good_df, column, 12, 'not_specified_length',
            index_out=True)
    good_df = good_df.loc[~non_12_char_index]
    
    # see if any phone numbers do NOT begin with 91
    num_bad_prefix, bad_prefix_df = string_contains(
            good_df, column, '^(?!91).+', 'non_91_prefix')

    # remove any remaining duplicate numbers, but keep first instance
    num_pre_dedup = len(good_df.index)
    good_df.drop_duplicates(subset=column, keep='first', inplace=True)
    logging.info('Found total of %i in dataset of %i for test: %s'
                 % (num_pre_dedup - len(good_df.index),
                    num_pre_dedup, 'duplicated_numbers'))

    logging.info('%i cases in dataset of %i have good phone numbers (%i percent)'
                 % (len(good_df.index), len(df.index), 100 * len(good_df.index) / len(df.index)))

    # prep output depending on user preference
    if split_error_df is True:
        return good_df, non_numeric_df, non_12_char_df, bad_prefix_df
    else:
        # combine all bad df's into one
        bad_frames = [non_numeric_df, non_12_char_df, bad_prefix_df]
        bad_df = pd.concat(bad_frames)
        return good_df, bad_df


def analyze_phone_data(df, output_dict, deep_dive_dups=False):
    '''
    Analyze phone data.  Runs through dataframe of phone data, yielding stats

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
    phone_value_counts = df['contact_phone_number'].value_counts()
    duplicate_series = phone_value_counts[phone_value_counts != 1]
    num_verified = len(df.index)

    # duplicates and unique nums
    num_duplicates = len(duplicate_series.index)
    unique_phone = df['contact_phone_number'].nunique()
    top_dups = phone_value_counts.index[0:15].tolist()
    top_dup_counts = phone_value_counts[0:15].tolist()
    logging.info('Out of verified numbers...')
    logging.info('%i unique numbers (%i percent of verified #s)'
                 % (unique_phone, int(unique_phone * 100 / num_verified)))
    logging.info('%i duplicated numbers (%i percent of verified #s)'
                 % (num_duplicates, int(num_duplicates * 100 / num_verified)))

    # see if phone_number isn't contact_phone_number
    diff_num_df = df[('91' + df['phone_number']) != df['contact_phone_number']]
    num_mismatch = len(diff_num_df.index)
    diff_num_df['error'] = 'phone_num_mismatch'
    logging.info('%i numbers are different between phone_number and '
                 'contact_phone_number' % num_mismatch)

    # see if any character is NOT 0-9 in the phone number
    num_non_numeric, non_numeric_df = string_contains(
            df, 'contact_phone_number', '\D+', 'non_numeric_char')

    # see if any phone numbers do NOT begin with 91
    num_bad_prefix, bad_prefix_df = string_contains(
            df, 'contact_phone_number', '^(?!91).+', 'non_91_prefix')

    # need to make sure the language code is either 'tel', 'mar', 'hin', 'en'
    lang_list = ['hin', 'mar', 'tel', 'en']
    bad_lang_code_df = df[~df['language_code'].isin(lang_list)]
    num_bad_lang_code = len(bad_lang_code_df.index)
    bad_lang_code_df['error'] = 'bad_lang_code'
    logging.info('%i numbers have bad language code' % num_bad_lang_code)

    # numbers that are only 10 digits, other oddities
    num_91_only = phone_value_counts.loc['91']
    logging.info('%i verified numbers that are just 91' % num_91_only)

    num_non_ten_char, non_ten_char_df = string_length(
            df, 'phone_number', 10, 'number_not_ten_char')

    num_123456789, df_123456789 = string_contains(
            df, 'contact_phone_number', '123456789', 'ascending_sequence')
    num_987654321, df_987654321 = string_contains(
            df, 'contact_phone_number', '987654321', 'descending_sequence')

    output_dict.update({'unique_phone': unique_phone,
                        'num_duplicates': num_duplicates,
                        'num_91_only': num_91_only,
                        'num_123456789': num_123456789,
                        'num_mismatch': num_mismatch,
                        'num_non_numeric': num_non_numeric,
                        'num_bad_prefix': num_bad_prefix,
                        'num_bad_lang_code': num_bad_lang_code,
                        'num_non_ten_char': num_non_ten_char,
                        'top_dups': top_dups,
                        'top_dup_counts': top_dup_counts,
                        'num_987654321': num_987654321})

    logging.info('15 most duplicated phone numbers:')
    logging.info(phone_value_counts.head(n=15))

    # create list of bad numbers by state. drop phi rows.
    error_frames = [diff_num_df, non_numeric_df, bad_prefix_df,
                    bad_lang_code_df, non_ten_char_df, df_123456789,
                    df_987654321]
    error_list = pd.concat(error_frames)

    # investigate further into duplicates
    if deep_dive_dups:
        # get dataframe that only has duplicate phone numbers.
        # group and count to see if all have same owner_id/caseid/household
        duplicate_index = df['contact_phone_number'].isin(
                duplicate_series.index.tolist())
        duplicate_df = df[duplicate_index]
        duplicate_df = duplicate_df.drop(
                ['closed', 'opened_date', 'last_modified_date',
                 'language_code', 'phone_number',
                 'contact_phone_number_is_verified', 'has_aadhar',
                 'aadhar_number', 'raw_aadhar_string'], 1)

        duplicate_df['count'] = 1
        dup_table = duplicate_df.groupby(['contact_phone_number']).agg(
                {'count': np.sum, 'owner_id': pd.Series.nunique,
                 'caseid': pd.Series.nunique,
                 'indices.household': pd.Series.nunique})

        unique_df_owner = df['owner_id'].nunique()
        unique_df_hh = df['indices.household'].nunique()
        unique_dup_owner = dup_table['owner_id'].nunique()
        unique_dup_hh = dup_table['indices.household'].nunique()
        dup_table['repeated_hh'] = dup_table['count'] - dup_table['indices.household']
        dup_table['repeated_owner'] = dup_table['count'] - dup_table['owner_id']

        num_repeated_hh = dup_table['repeated_hh'][dup_table['repeated_hh'] > 0].count()
        num_repeated_owner = dup_table['repeated_owner'][dup_table['repeated_owner'] > 0].count()
        num_all_repeated_hh = dup_table['repeated_hh'][dup_table['repeated_hh'] == dup_table['indices.household']].count()
        num_all_repeated_owner = dup_table['repeated_owner'][dup_table['repeated_owner'] == dup_table['owner_id']].count()

        logging.info('Out of %i numbers that have duplicate entries:'
                     % num_duplicates)
        logging.info('%i share at least one household for a given '
                     'duplicate (%i percent)' % (num_repeated_hh, int(num_repeated_hh * 100 / num_duplicates)))
        logging.info('%i share all households for a given duplicate '
                     '(%i percent)' % (num_all_repeated_hh, int(num_all_repeated_hh * 100 / num_duplicates)))
        logging.info('%i unique households in the duplicate dataset, out '
                     'of %i unique in the clean phone dataset (%i percent)' % (unique_dup_hh, unique_df_hh, int(unique_dup_hh * 100 / unique_df_hh)))
        logging.info('%i share at least one ownerid for a given duplicate '
                     '(%i percent)' % (num_repeated_owner, int(num_repeated_owner * 100 / num_duplicates)))
        logging.info('%i share all ownerid for a given duplicate '
                     '(%i percent)' % (num_all_repeated_owner, int(num_all_repeated_owner * 100 / num_duplicates)))
        logging.info('%i unique ownerids in the duplicate dataset, out '
                     'of %i unique in the clean phone dataset (%i percent)'
                     % (unique_dup_owner, unique_df_owner, int(unique_dup_owner * 100 / unique_df_owner)))

        output_dict.update({'num_repeated_hh': num_repeated_hh,
                            'num_all_repeated_hh': num_all_repeated_hh,
                            'num_repeated_owner': num_repeated_owner,
                            'num_all_repeated_owner': num_all_repeated_owner,
                            'unique_df_owner': unique_df_owner,
                            'unique_df_hh': unique_df_hh,
                            'unique_dup_owner': unique_dup_owner,
                            'unique_dup_hh': unique_dup_hh})
    return output_dict, error_list
