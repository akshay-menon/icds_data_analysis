# -*- coding: utf-8 -*-
"""
Created on Tue Aug 08 20:00:46 2017

@author: theism
"""
import logging
import verhoeff
import re
import numpy as np
import pandas as pd
from case_func import *


def clean_aadhar_data(df, output_dict):
    '''
    Clean aadhar data.  Remove blank and NaN aadhar numbers.

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
    logging.info('Cleaning aadhar data...')
    orig_aadhar_rows = len(df.index)
    logging.info('%i rows found' % orig_aadhar_rows)

    # remove blank and nan aadhar numbers
    if 'aadhar_number' in df.columns.tolist():
        skipped_index = df['aadhar_number'] == '---'
        num_skipped = skipped_index.value_counts()[True]
        df = df[~skipped_index]
        logging.info('%i cases with skipped aadhar question found' % num_skipped)

        blank_index = df['aadhar_number'].isnull()
        num_blank = blank_index.value_counts()[True]
        df = df[~blank_index]
        logging.info('%i cases with blank aadhar number found' % num_blank)
    else:
        logging.info('No aadhar number column found.  Not removing blanks or skips.')
        num_blank = np.nan
        num_skipped = np.nan

    num_clean_aadhar_nums = len(df.index)
    logging.info('%i cases with aadhar numbers (out of %i cases, %i percent)'
                 % (num_clean_aadhar_nums, orig_aadhar_rows,
                    int(num_clean_aadhar_nums * 100 / orig_aadhar_rows)))
    logging.info('Returning dataframe with %i rows' % num_clean_aadhar_nums)
    output_dict.update({'num_blank': num_blank, 'num_skipped': num_skipped,
                        'num_clean_aadhar_nums': num_clean_aadhar_nums})
    return df, output_dict


def only_valid_aadhar(df, column, split_error_df=False):
    '''
    Creates a dataframe that is only 'good' aadhar numbers.  Checks for
    non-numeric characters, 12 digit length, checksum of 12th digit, and
    removes duplicates.

    Parameters
    ----------
    df : pandas dataframe
      Dataframe to analyze

    column : string
      Name of column to analyze for aadhar goodness

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

    fail_checksum_df : pandas dataframe
      (Optional) Dataframe of all entries that failed verhoeff checksum
    '''
    logging.info('Looking for only valid aadhar numbers...')
    # remove blank and nan aadhar numbers
    if 'aadhar_number' in df.columns.tolist():
        skipped_index = df['aadhar_number'] == '---'
        if True in skipped_index.value_counts():
            num_skipped = skipped_index.value_counts()[True]
        else:
            num_skipped = 0
        good_df = df[~skipped_index]
        logging.info('%i cases with skipped aadhar question found' % num_skipped)

        blank_index = good_df['aadhar_number'].isnull()
        if True in blank_index.value_counts():
            num_blank = blank_index.value_counts()[True]
        else:
            num_blank = 0
        good_df = good_df[~blank_index]
        logging.info('%i cases with blank aadhar number found' % num_blank)
    else:
        good_df = df.copy()
        logging.info('No aadhar number column found.  Not removing blanks or skips.')

    # see if any character is NOT 0-9 in number
    num_non_numeric, non_numeric_df, non_numeric_index = string_contains(
            good_df, column, '\D+', 'non_numeric_char', index_out=True)
    good_df = good_df.loc[~non_numeric_index]

    # check string length
    num_non_12_char, non_12_char_df, non_12_char_index = string_length(
            good_df, column, 12, 'not_specified_length',
            index_out=True)
    good_df = good_df.loc[~non_12_char_index]

    # first 11 digits of Aadhar random.  12th is a checksum.
    # https://www.quora.com/What-is-the-structure-of-ones-Aadhar-Card-UID-number
    # analyze verhoeff checksum numbers of remaining_good_df
    good_checksum_index = good_df[column].apply(
            verhoeff.validateVerhoeff)
    fail_checksum_df = good_df.loc[~good_checksum_index]
    fail_checksum_df['error'] = 'failed_checksum'
    num_failed_checksum = len(fail_checksum_df.index)
    logging.info('%i that failed checksum (%i percent of assumed good (%i) '
                'aadhar entries)' % (num_failed_checksum,
                int(num_failed_checksum * 100 / len(good_df.index)), len(good_df.index)))
    good_df = good_df.loc[good_checksum_index]

    # remove any remaining duplicate aadhar_numbers, but keep first instance
    num_pre_dedup = len(good_df.index)
    good_df.drop_duplicates(subset=column, keep='first', inplace=True)
    logging.info('Found total of %i in dataset of %i for test: %s'
                 % (num_pre_dedup - len(good_df.index),
                    num_pre_dedup, 'duplicated_numbers'))

    logging.info('%i cases in dataset of %i have good aadhar numbers (%i percent)'
                 % (len(good_df.index), len(df.index), 100 * len(good_df.index) / len(df.index)))

    # prep output depending on user preference
    if split_error_df is True:
        return good_df, non_numeric_df, non_12_char_df, fail_checksum_df
    else:
        # combine all bad df's into one
        bad_frames = [non_numeric_df, non_12_char_df, fail_checksum_df]
        bad_df = pd.concat(bad_frames)
        return good_df, bad_df


def analyze_aadhar_data(df, output_dict):
    '''
    Analyze aadhar data.  Runs through dataframe of aadhar data, yielding stats

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
    logging.info('Analyzing Aadhar data...')
    logging.info('Found %i rows' % len(df.index))

    # duplicates and unique nums
    value_counts = df['aadhar_number'].value_counts()
    duplicate_series = value_counts[value_counts != 1]
    num_repeated_twice = value_counts[value_counts == 2].shape[0]
    num_duplicates = len(duplicate_series.index)
    num_unique = df['aadhar_number'].nunique()
    top_dups = value_counts.index[0:15].tolist()
    top_dup_counts = value_counts[0:15].tolist()
    logging.info('%i unique numbers (%i percent of verified #s)'
                 % (num_unique, int(num_unique * 100 / len(df.index))))
    logging.info('%i duplicated numbers (%i percent of verified #s)'
                 % (num_duplicates, int(num_duplicates * 100 / len(df.index))))
    logging.info('%i numbers repeated exactly twice numbers '
                 '(%i percent of duplicated #s)' % (num_repeated_twice,
                 int(num_repeated_twice * 100 / num_duplicates)))

    # CURIOSITIES TO FLAG (not removed from 'good' dataset):
    # bad age bracket - could be bad dob entry, but flagging
    num_99, num_99_df = num_in_age_bracket(df)

    # see if sequences in number
    num_123456789, df_123456789 = string_contains(
            df, 'aadhar_number', '123456789', 'ascending_sequence')
    num_987654321, df_987654321 = string_contains(
            df, 'aadhar_number', '987654321', 'descending_sequence')
    
    # check start sequences - cant be 1 or 0
    num_starts_1, df_starts_1 = string_starts_with(
            df, 'aadhar_number', '1', 'starts_with_1')
    num_starts_0, df_starts_0 = string_starts_with(
            df, 'aadhar_number', '0', 'starts_with_0')

    # test for repeated digits via regex for more than 7 of the same
    # digits in a row (looking for 999999999)
    repeat_regex = re.compile(r'([0-9])\1{6,}')
    repeat_index = df['aadhar_number'].apply(
            lambda x: apply_regex(repeat_regex, x))
    df_repeat_dig = df[repeat_index]
    df_repeat_dig['error'] = 'repeated_digits'
    num_repeat_dig = len(df_repeat_dig.index)
    logging.info('Found total of %i in dataset of %i for test: %s'
                 % (num_repeat_dig, len(df.index), 'repeated_digits'))

    # estimate number good aadhar cases
    # Good = number of unique entries with non-numeric values that are
    # 12 digits in length and satisfy checksum conditions
    remaining_good_df, non_numeric_df, non_12_char_df, fail_checksum_df = only_valid_aadhar(
            df, 'aadhar_number', split_error_df=True)
    num_non_numeric = len(non_numeric_df.index)
    num_non_12_char = len(non_12_char_df.index)
    num_failed_checksum = len(fail_checksum_df.index)
    num_good_aadhar = len(remaining_good_df.index)
    logging.info('%i estimated to have good aadhar numbers '
                 '(%i percent of clean cases with aadhar numbers)'
                 % (num_good_aadhar,
                    int(num_good_aadhar * 100 / len(df.index))))

    # DO SOME ANALYSIS ON ATTEMPTED VERSUS CAPTURED SCANS
    # attempted aadhar scan of card (raw_aadhar_info not blank)
    num_null_raw_aadhar, null_raw_df, has_raw_df = string_not_null(
            df, 'raw_aadhar_string', 'no_raw_aadhar_string')
    num_attempted_scan = len(has_raw_df.index)
    logging.info('%i attempted scans (has raw aadhar string value) '
                 '(%i percent of cases with aadhar)' % (num_attempted_scan,
                      int(num_attempted_scan * 100 / len(df.index))))

    # of attempted scans, scans that returned invalid xml for 2d barcode
    num_invalid_2dbarcodes, invalid_2dbarcode_df, invalid_2dbarcode_index = string_not_contains(
            has_raw_df, 'raw_aadhar_string', 'uid=', 'invalid_2dbarcode_scan', index_out=True)
    valid_2d_barcode_df = has_raw_df[~invalid_2dbarcode_index]
    num_valid_2d_scans = num_attempted_scan - num_invalid_2dbarcodes
    logging.info('%i scans with valid 2d barcodes (%i percent of attempted scans)'
                 % (num_valid_2d_scans, int(num_valid_2d_scans * 100 / num_attempted_scan)))

    # of scans with raw values, that aren't 2d barcodes
    remaining_raw_df = has_raw_df[invalid_2dbarcode_index]
    # of remaining raw, how many good 1d barcodes
    logging.info('Analyzing good aadhar numbers from 1d barcode scans')
    remaining_raw_df, invalid_1dbarcode_df = only_valid_aadhar(
            remaining_raw_df, 'raw_aadhar_string', split_error_df=False)
    invalid_1dbarcode_df['error'] = 'invalid_1d_barcode_scan'
    num_valid_1d_scans = len(remaining_raw_df.index)
    logging.info('%i scans with valid 1d barcodes (%i percent of attempted scans)'
                 % (num_valid_1d_scans, int(num_valid_1d_scans * 100 / num_attempted_scan)))

    num_invalid_scans = num_attempted_scan - num_valid_2d_scans - num_valid_1d_scans
    logging.info('%i invalid scans (%i percent of attempted scans)'
                 % (num_invalid_scans, int(num_invalid_scans * 100 / num_attempted_scan)))

    # good 2d scan, but manually changed aadhar number
    valid_2d_barcode_df['num_from_raw'] = valid_2d_barcode_df['raw_aadhar_string'].str.extract('(?<=uid=")(\d+)')
    mismatch_2d_scan_df = valid_2d_barcode_df[valid_2d_barcode_df['aadhar_number'] != valid_2d_barcode_df['num_from_raw']]
    num_mismatch_2d_scan = len(mismatch_2d_scan_df.index)
    mismatch_2d_scan_df['error'] = 'good2d_scan_manually_changed'
    logging.info('%i valid 2d scans that were manually changed (%i percent of valid 2d scans)'
                 % (num_mismatch_2d_scan, int(num_mismatch_2d_scan * 100 / num_valid_2d_scans)))

    # good 1d scan, but manually changed aadhar number
    mismatch_1d_scan_df = remaining_raw_df[remaining_raw_df['aadhar_number'] != remaining_raw_df['raw_aadhar_string']]
    num_mismatch_1d_scan = len(mismatch_1d_scan_df.index)
    mismatch_1d_scan_df['error'] = 'good1d_scan_manually_changed'
    logging.info('%i valid 1d scans that were manually changed (%i percent of valid 1d scans)'
                % (num_mismatch_1d_scan, int(num_mismatch_1d_scan * 100 / num_valid_1d_scans)))

    # bad 1d scan, and manually changed aadhar number
    mismatch_bad1d_scan_df = invalid_1dbarcode_df[invalid_1dbarcode_df['aadhar_number'] != invalid_1dbarcode_df['raw_aadhar_string']]
    num_mismatch_bad1d_scan = len(mismatch_bad1d_scan_df.index)
    mismatch_bad1d_scan_df['error'] = 'bad1d_scan_manually_changed'
    logging.info('%i bad 1d scans that were manually changed (%i percent of valid 1d scans)'
                % (num_mismatch_bad1d_scan,
                   int(num_mismatch_bad1d_scan * 100 / num_valid_1d_scans)))

    num_manually_changed = num_mismatch_2d_scan + num_mismatch_1d_scan + num_mismatch_bad1d_scan
    logging.info('%i manually changed numbers (%i of attempted scans)' %
                 (num_manually_changed, int(
                         num_manually_changed * 100 / num_attempted_scan)))

    '''
    # of attempted scans with invalid xml, how many manually changed the xml
    mismatch_invalid_xml_df = invalid_xml_df[invalid_xml_df[
            'aadhar_number'] != invalid_xml_df['raw_aadhar_string']]
    mismatch_invalid_xml_df['error'] = 'mismatch_num_to_invalid_xml'
    num_mismatch_invalid_xml = len(mismatch_invalid_xml_df.index)
    logging.info('%i have invalid xml scans and manually changed aadhar
                 numbers (%i of invalid scans)' % (num_mismatch_invalid_xml,
                 int(num_mismatch_invalid_xml * 100 / num_invalid_xml)))

    # of attempted scans with 2d barcodes, how many have different 
    # aadhar numbers compared to uid in xml (some don't have returned xml)
    # proxy for if scanned and fixed.
    # can get from (1) bad xml scan or (2) corrected from good scan
    num_valid_2dbarcodes_xml, valid_xml_df = string_contains(
            has_raw_df, 'raw_aadhar_string', 'uid=', 'valid_xml_scan')
    valid_xml_df['num_from_raw'] = valid_xml_df[
            'raw_aadhar_string'].str.extract('(?<=uid=")(\d+)')
    mismatch_raw_input_df = valid_xml_df[valid_xml_df[
            'aadhar_number'] != valid_xml_df['num_from_raw']]
    mismatch_raw_input_df['error'] = 'mismatch_num_to_xml_num'
    num_mismatch_valid_xml = len(mismatch_raw_input_df.index)
    logging.info('%i have valid xml scans and manually changed aadhar
                 numbers % num_mismatch_valid_xml)
    '''

    # structure output dict
    output_dict.update({'num_duplicates': num_duplicates,
                        'num_unique': num_unique,
                        'num_repeated_twice': num_repeated_twice,
                        'num_99': num_99,
                        'num_non_numeric': num_non_numeric,
                        'num_failed_checksum': num_failed_checksum,
                        'num_123456789': num_123456789,
                        'num_987654321': num_987654321,
                        'num_starts_1': num_starts_1,
                        'num_starts_0': num_starts_0,
                        'num_repeat_dig': num_repeat_dig,
                        'num_non_12_char': num_non_12_char,
                        'num_attempted_scan': num_attempted_scan,
                        'num_valid_2d_scans': num_valid_2d_scans,
                        'num_valid_1d_scans': num_valid_1d_scans,
                        'num_invalid_scans': num_invalid_scans,
                        'num_mismatch_2d_scan': num_mismatch_2d_scan,
                        'num_mismatch_1d_scan': num_mismatch_1d_scan,
                        'num_mismatch_bad1d_scan': num_mismatch_bad1d_scan,
                        'num_manually_changed': num_manually_changed,
                        'top_dups': top_dups,
                        'top_dup_counts': top_dup_counts,
                        'num_good_aadhar': num_good_aadhar})

    # create list of bad numbers by state. drop phi rows.
    error_frames = [non_numeric_df, non_12_char_df, df_123456789,
                    df_987654321, df_starts_1, df_starts_0,
                    df_repeat_dig, invalid_2dbarcode_df,
                    invalid_1dbarcode_df, mismatch_2d_scan_df,
                    mismatch_1d_scan_df, mismatch_bad1d_scan_df,
                    fail_checksum_df, num_99_df]
    error_list = pd.concat(error_frames)

    return output_dict, error_list
