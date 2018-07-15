# -*- coding: utf-8 -*-
"""
Helper Functions for Files

these functions support generic manipulation of ICDS files for data analysis

@author: Matt Theis, July 2017
"""
import os
import pandas as pd
import logging
import logging.config
import datetime
from requests.auth import HTTPBasicAuth
import requests, zipfile
from io import BytesIO
import shutil
from dateutil.parser import parse
import hashlib
import settings
from settings import DATA_DIR, OUTPUT_DIR

location_file_dir = os.path.join(DATA_DIR, 'static-awc_location.csv')
credential_path = os.path.join(os.path.dirname(DATA_DIR), 'Admin' ,'user_info.csv')

def data_file_list(directory, regex):
    '''
    Go through a directory, get files that match a regex, return list of files.

    Parameters
    ----------
    directory : string
      Full path to directory

    regex : regex object
      Regex used to specify filenames of desired csv files

    Returns
    -------
    output : list of strings
      List of file names in specified directory

    '''
    output = []
    orig_dir = os.getcwd()
    os.chdir(directory)
    files = os.listdir('.')
    for f in files:
        mo = regex.search(f)
        if mo is not None:
            output.append(mo.group())
    os.chdir(orig_dir)
    return output

def _hash_from_filesize_and_cols(directory, regex, cols_to_use):
  '''
  Return a hash of the summed filesizes of the files in the directory.
  This is used as a rough metric to see if anything has changed in
  the directory. Also included are the fields to extract.

  NOTE: right now there is no hash. It just returns the summed filesizes,
  which should be good enough for our purposes

  NOTE: creating a different HDF for each 'view' of the data leads to further
  duplication of data, but for now that's acceptable.

  Parameters
  ----------
  directory : string
    Full path to directory

  regex : regex object
    Regex used to specify filenames of desired csv files
  
  Returns
  -------
  output : string
    hash for this set of files
  '''
  try:
    file_list = data_file_list(directory, regex)
    orig_dir = os.getcwd()
    os.chdir(directory)
    sizes = [os.path.getsize(f) for f in file_list]
    os.chdir(orig_dir)
    
    sorted_cols = list(cols_to_use) if cols_to_use else []
    sorted_cols.sort()
    return hashlib.sha1(
      (str(sum(sizes)) + ''.join(sorted_cols)).encode()).hexdigest()
  except Exception as err:
    logging.error('An exception happened: ' + str(err))
    os.chdir(orig_dir)
    raise


def forms_to_df(directory, regex, date_cols=None, cols_to_use=None):
  '''
  Load in form data to a single dataframe. This is an optimization
  function that checks for an existing hd5 file and hash before
  passing off the real work to csv_files_to_df.

  Parameters
  ----------
  directory : string
    Full path to directory

  regex : regex object
    Regex used to specify filenames of desired csv files

  date_cols : list of strings
    Import specific columns in datetime format (optional, defaults to None)

  cols_to_use : list of string
    Import a subset of columns (optional, defaults to None)

  Returns
  -------
  output : pandas dataframe
    Dataframe for these forms
  '''
  try:
    orig_dir = os.getcwd()
    os.chdir(directory)
    current_hash = _hash_from_filesize_and_cols(directory, regex, cols_to_use=cols_to_use)
    hash_file = open(settings.HASH_FILE, 'r')
    hash_values = hash_file.readlines()

    assert current_hash+'.hdf\n' in hash_values

    logging.info('Loading cached data from %s.hdf' % (current_hash))
    df = pd.read_hdf('%s.hdf' % (current_hash),
                      settings.HDF_KEY, 
                      usecols=cols_to_use,
                      parse_dates=date_cols,
                      infer_datetime_format=True,
                      low_memory=False)
    df = optimize_df_memory(df)
    os.chdir(orig_dir)
    return df
  except IOError as ex:
    logging.info('HDF5 or hash file not found in "%s", loading from CSV files. Details:\n%s' % (directory, ex))
  except AssertionError as ex:
    logging.info('New hash for files in "%s" has changed, reloading from CSV files' % (directory))
  
  return csv_files_to_df(directory, regex, date_cols=date_cols, cols_to_use=cols_to_use, dtypes=None, save_hdf=True)

def csv_files_to_df(directory, regex, date_cols=None, cols_to_use=None, dtypes=None, save_hdf=False):
    '''
    Combine all csv files in directory to a single dataframe.

    Parameters
    ----------
    directory : string
      Full path to directory

    regex : regex object
      Regex used to specify filenames of desired csv files

    date_cols : list of strings
      Import specific columns in datetime format (optional, defaults to None)

    cols_to_use : list of string
      Import a subset of columns (optional, defaults to None)
      
    dtypes : dictionary
      Dictionary of column names and known data types
    
    save_hdf : boolean
      Whether we should save a hash and hd5 export of the final dataframe

    Returns
    -------
    output : pandas dataframe
      Dataframe of combined csv files
    '''
    try:
        file_list = data_file_list(directory, regex)
        frames = []
        orig_dir = os.getcwd()
        os.chdir(directory)
        # append frames to a list of frames, then concat into one large frame
        for data_file in file_list:
            frame = pd.DataFrame()
            frame = frame.fillna('')
            frame = pd.read_csv(data_file, usecols=cols_to_use,
                                parse_dates=date_cols,
                                infer_datetime_format=True,
                                dtype = dtypes)
            frames.append(frame)
            logging.info('Adding %s with length %i rows' %
                          (data_file, len(frame.index)))
        df = pd.concat(frames, ignore_index=True, copy=False)

        if save_hdf:
          current_hash = _hash_from_filesize_and_cols(directory, regex, cols_to_use)  
          df.to_hdf('%s.hdf' % (current_hash), settings.HDF_KEY)
          f = open(settings.HASH_FILE, 'a')
          f.write('%s.hdf\n' % (current_hash))
          f.close()
          
        #TODO - to_hdf not using optimized df b/c can't handle categories.  Can save format='table'
        #as setting, but causes other errors when importing due to 'operands broadcast together due to array size
        df = optimize_df_memory(df)
        logging.info('Total combined length is %i rows\n' % len(df.index))
        os.chdir(orig_dir)
        return df
    except Exception as err:
        logging.error('An exception happened: ' + str(err))
        raise


def combine_csvs(directory, new_directory, filename, regex, date_cols=None, cols_to_use=None):
    '''
    Combine all csv files in directory to a single csv and saves to same directory.

    Parameters
    ----------
    directory : string
      Full path to directory with data files
    new_directory : string
      Full path to directory where you want to save the new file
    regex : regex object
      Regex used to specify filenames of desired csv files
    filename: string
      What you want to call your combined csv file
    date_cols : list of strings
      Import specific columns in datetime format (optional, defaults to None)
    cols_to_use : list of string
      Import a subset of columns (optional, defaults to None)

    Returns
    -------
    output : pandas dataframe
      Dataframe of combined csv files
    '''
    big_df = csv_files_to_df(directory, regex, date_cols, cols_to_use)
    output_file = os.path.join(new_directory, filename) + '.csv'
    big_df.to_csv(output_file)
    logging.info('Saved output to %s' % output_file)


# hat tip: https://gist.github.com/jrivero/1085501
def split_csvs(filehandler, delimiter=',', row_limit=10000, 
    output_name_template='output_%s.csv', output_path='.', keep_headers=True):
    """
    Splits a CSV file into multiple pieces.
    
    A quick bastardization of the Python CSV library.
    Arguments:
        `row_limit`: The number of rows you want in each output file. 10,000 by default.
        `output_name_template`: A %s-style template for the numbered output files.
        `output_path`: Where to stick the output files.
        `keep_headers`: Whether or not to print the headers in each output file.
    Example usage:
    
        >> from toolbox import csv_splitter;
        >> csv_splitter.split_csvs(open('/home/ben/input.csv', 'r'));
    
    """
    import csv
    reader = csv.reader(filehandler, delimiter=delimiter)
    current_piece = 1
    current_out_path = os.path.join(
         output_path,
         output_name_template  % current_piece
    )
    current_out_writer = csv.writer(open(current_out_path, 'w'), delimiter=delimiter)
    current_limit = row_limit
    if keep_headers:
        headers = reader.next()
        current_out_writer.writerow(headers)
    for i, row in enumerate(reader):
        if i + 1 > current_limit:
            current_piece += 1
            current_limit = row_limit * current_piece
            current_out_path = os.path.join(
               output_path,
               output_name_template  % current_piece
            )
            current_out_writer = csv.writer(open(current_out_path, 'w'), delimiter=delimiter)
            if keep_headers:
                current_out_writer.writerow(headers)
        current_out_writer.writerow(row)


def find_and_split_csvs(directory, file_limit=1000000000):
    logging.info('Testing for csv files larger than %i bytes' % file_limit)
    orig_dir = os.getcwd()
    os.chdir(directory)
    file_list = os.listdir(directory)
    for myfile in file_list:
        test_file = os.path.join(directory, myfile)
        if os.path.getsize(test_file) > file_limit:
            logging.info('%s exceeds limit.  Splitting file' % test_file)
            split_csvs(open(test_file), row_limit=1000000, output_name_template = myfile[:-4] + '%s.csv')
    os.chdir(orig_dir)
    return


def add_locations(df, left_index_column=None, location_column_names=['doc_id',
                  'awc_name', 'block_name', 'district_name', 'state_name'],
                  refresh_loc=False):
    '''
    Add location columns to an existing dataframe (ie-awc/block/district/etc).

    Parameters
    ----------
    df : pandas dataframe
      Dataframe to add location columns to
    left_index_column : string
      Name of column to use for location lookup (Optional, default to None -
      if use None, will use the index column of the input df as the lookup)
    location_column_names : list of strings
      Location columns to add.  (Optional, defaults to doc_id, awc_name,
      block_name, district_name, state_name).  
    refresh_loc : boolean
      Will update the location fixture with the latest information if True

    Returns
    -------
    output : pandas dataframe
      Dataframe with added location columns.
    '''
    if refresh_loc == True:
        refresh_locations()
    try:
        orig_df_columns = df.columns.tolist()
        known_dtype = {'awc_name':'object', 'block_name':'category',
                       'district_name':'category', 'state_name':'category'}
        if location_column_names in orig_df_columns:
            logging.info('WARNING - column names to add already exist')
        location_df = pd.read_csv(location_file_dir,
                                  index_col=location_column_names[0],
                                  usecols=location_column_names,
                                  dtype=known_dtype)
        location_df = optimize_df_memory(location_df)
        if left_index_column is not None:
            df = pd.merge(df, location_df, left_on=left_index_column,
                          right_index=True, how='left')
        else:
            location_df = location_df.groupby(level=0).last()
            df = pd.merge(df, location_df, left_index=True,
                          right_index=True, how='left')
        desired_columns = location_column_names[1:] + orig_df_columns
    except:
       logging.info('ERROR - unable to find location file, not adding \
                    location columns.  Looking in %s', location_file_dir)
       raise
    return df[desired_columns]


def add_locations_by_username(df, location_column_names=['awc_site_code',
                  'awc_name', 'block_name', 'district_name', 'state_name'],
                  refresh_loc=False):
    '''
    Similar to add_locations, but for forms where location_id isn't available
    but 'username' is.  Takes username and adds location columns to an existing
    for locations (ie-awc/block/district/etc).

    Parameters
    ----------
    df : pandas dataframe
      Dataframe to add location columns to
    location_column_names : list of strings
      Location columns to add.  (Optional, defaults to awc_site_code, awc_name,
      block_name, district_name, state_name).  NEED awc_site_code to match on
      username
    refresh_loc : boolean
      Will update the location fixture with the latest information if True

    Returns
    -------
    output : pandas dataframe
      Dataframe with added location columns.
    '''
    if refresh_loc == True:
        refresh_locations()
    try:
        orig_df_columns = df.columns.tolist()
        known_dtype = {'awc_name':'object', 'block_name':'category',
                       'district_name':'category', 'state_name':'category'}
        if location_column_names in orig_df_columns:
            logging.info('WARNING - column names to add already exist')
        location_df = pd.read_csv(location_file_dir,
                                  usecols=location_column_names,
                                  dtype=known_dtype)
        location_df = optimize_df_memory(location_df)
        if 'awc_site_code' not in location_column_names:
            logging.info('WARNING - awc_site_code required in location column list')
        location_df['awc_site_code'] = location_df['awc_site_code'].astype(str)
        
        # format the username appropriately, dropping any leading zeros
        df['username'] = df['username'].astype(str)
        df['username_fmt'] = df['username'].apply(lambda x: x[1:] if x[0] == '0' else x)

        # add location information for each user        
        output_df = pd.merge(df, location_df, left_on='username_fmt', right_on='awc_site_code', how='left')
    except:
       logging.info('ERROR - unable to find location file, not adding '
                    'location columns.  Looking in %s', location_file_dir)
       raise
    return output_df


def num_by_location(column_name, filter_name):
    '''
    Feed the function the name of the column in the location fixture and the
    filter to apply, and the function will return the number that match the
    filter in the location fixture - ie, how many users in state X

    Parameters
    ----------
    column_name : string
      Name of the column in the location fixture to look in
    filter_name : string
      Filter to apply in the column (ie, the name of the state to look for)

    Returns
    -------
    num_out : integer
      Number in input dataframe that match the specified filter
    '''
    try:
        known_dtype = {'awc_name':'object', 'block_name':'category',
                       'district_name':'category', 'state_name':'category'}
        location_df = pd.read_csv(location_file_dir, index_col='doc_id', low_memory=False, dtype=known_dtype)
        location_df = optimize_df_memory(location_df)
        num_out = location_df[location_df[column_name] == filter_name].count()[column_name]
    except:
        logging.info('ERROR - unable to find location file, not adding \
                     location columns.  Looking in %s', location_file_dir)
        raise
    return num_out


def folder_name_to_location(full_name):
    '''
    Returns full state names from acceptable suffixes,
    ie - xxx-mp -> Madya Pradesh

    Parameters
    ----------
    full_name : string
      Full file path with an expected suffix at the end

    Returns
    -------
    string : string
      Full location name based on suffix lookup.
    '''
    suffix = full_name[full_name.find('-')+1:]
    lookup_dict = {'ap': 'Andhra Pradesh', 'bihar': 'Bihar',
                   'ch': 'Chhattisgarh', 'jh': 'Jharkhand',
                   'mp': 'Madhya Pradesh', 'raj': 'Rajasthan',
                   'up': 'Uttar Pradesh', 'mah': 'Maharashtra',
                   'user': 'User', 'test': 'Test',
                   'ap2': 'Andhra Pradesh2'}
    if suffix in lookup_dict:
        return lookup_dict[suffix]
    else:
        return 'None'


def add_usertype_from_id(df, df_id_col):
    '''
    Based on id location column (like commcare_location_id),
    add a new column that shows id location type (aww/ls/block/district/state)

    Parameters
    ----------
    df : pandas dataframe
      Dataframe to append usertype column to

    df_id_col : string
      Name of column to use for location, ie commcare_location_id or owner_id

    Returns
    -------
    df : pandas dataframe
      Dataframe with 'location_type' column added.
    '''
    try:
        col_names = ['doc_id', 'supervisor_id', 'block_id',
                     'district_id', 'state_id']
        known_dtype = {'awc_name':'object', 'block_name':'category',
                       'district_name':'category', 'state_name':'category'}
        loc_df = pd.read_csv(location_file_dir, usecols=col_names, dtype=known_dtype)
        loc_df = optimize_df_memory(loc_df)
        aww_series = pd.Series('aww', loc_df['doc_id'].unique())
        ls_series = pd.Series('ls', loc_df['supervisor_id'].unique())
        block_series = pd.Series('block', loc_df['block_id'].unique())
        district_series = pd.Series('district', loc_df['district_id'].unique())
        state_series = pd.Series('state', loc_df['state_id'].unique())
        loc_series = pd.concat([aww_series, ls_series, block_series,
                                district_series, state_series])
        df['location_type'] = df[df_id_col].map(loc_series)
    except:
        logging.info('ERROR - unable to find location file, not adding \
                     location_type column.  Looking in %s', location_file_dir)
    return df


def start_logging(output_dir):
    '''
    Starts a log file.  logging.debug to a file, logging.info to the console
    from cookbook#logging-to-multiple-destinations

    Parameters
    ----------
    output_dir : file path string
      Dataframe to append usertype column to

    df_id_col : string
      Full path to directory to save logfile

    Returns
    -------
    None
    '''
    text_out = os.path.join(output_dir, ('log_text_' +
                            str(datetime.date.today()) + '.txt'))
    
    # create directory if doesn't exist already
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    # logging already happening? - if so, don't add more logging handlers
    if not len(logging.getLogger('').handlers):
        logging.basicConfig(level=logging.DEBUG,
                            format='%(asctime)s: %(levelname)-8s %(message)s',
                            datefmt='%m-%d-%y %H:%M',
                            filename=text_out)

        # define Handler which writes INFO messages or higher to the sys.stderr
        console = logging.StreamHandler()
        console.setLevel(logging.INFO)
        # set a format which is simpler for console use
        formatter = logging.Formatter('%(levelname)-8s %(message)s')
        console.setFormatter(formatter)
        # add the handler to the root logger
        logging.getLogger('').addHandler(console)
    return


def get_credentials(file_path, key):
    '''Gets a username and password stored in a csv file with the key/value
    pairs of type, user, password
    
    Parameters
    ----------
    file_path : string
      Path to the csv file where you have some credentials.  Csv file is 3
      columns, key lookup of credentials (use), username (user), and password
      (pass)
    key : string
      The lookup for your credentials in the first row

    Returns
    -------
    user : string
      Username
    password : string
      Password
    '''
    creds = pd.read_csv(file_path, index_col='use')
    user = creds.loc[key, 'user']
    password = creds.loc[key, 'pass']
    return user, password


def download_ucr(url, user, password, new_file_name, target_dir):
    '''Downloads a UCR file if given the url, credentials, and full location to
    save the filename.  Assumes is only one file in the downloaded UCR
    
    Parameters
    ----------
    url : string
      URL to the UCR.  Entering this in a web browser will cause the file to
      start downloading.
    user : string
      Username
    password : string
      Password
    new_file_name : string
      What you want to call your new file, including the file extension
    target_dir : string
      Path to the directory where you want to extract the file
    
    Returns
    -------
    None
    '''
    # go to the right place in commcare and download the file
    r = requests.get(url, auth=HTTPBasicAuth(user, password))
    try:
        r.raise_for_status()
    except Exception as exc:
        print('There was a problem: %s' % (exc))
    logging.info('Download complete')

    # its a zipfile, so unpack and save in target_dir
    logging.info('Unzipping file...')
    z = zipfile.ZipFile(BytesIO(r.content))
    cur_file_name = z.extract(z.namelist()[0], target_dir)
    shutil.move(cur_file_name, os.path.join(target_dir, new_file_name))
    logging.info('Moved new file %s to %s directory' % (new_file_name, target_dir))
    z.close()

def refresh_locations():
    '''Will delete and re-download the static location file from ucr'''
    logging.info('Refreshing data file: %s' % location_file_dir)
    try:
        location_download_link = 'https://www.icds-cas.gov.in/a/icds-cas/configurable_reports/data_sources/export/static-icds-cas-static-awc_location/?format=csv'
        user, password = get_credentials(credential_path, 'icds')

        # rename and verify download before delete - and see if file even exists
        old_file_name = location_file_dir[:-4] + 'OLD.csv'
        if os.path.isfile(location_file_dir):
            logging.info('Found older location file.  Getting latest data.')
            os.rename(location_file_dir, old_file_name)

        download_ucr(location_download_link, user, password, os.path.basename(location_file_dir), os.path.dirname(location_file_dir))
        if os.path.isfile(location_file_dir) and os.path.isfile(old_file_name):
            logging.info('Deleting old location file.  New one downloaded.')
            os.remove(old_file_name)
        return
    except Exception as err:
        logging.error('An exception happened: ' + str(err))
        raise

def iterate_ucr_download(ucr_name, my_filter, filter_list, target_dir):
    '''Downloads mulitple UCR data from HQ.  Use sparingly.
    Format of files is csv.
    
    Parameters
    ----------
    ucr_name : string
      Name of the UCR you want to download - like static-icds-cas-static-tasks_cases
    my_filter : string
      This the property within the UCR you want to filter by.  UCR data is way
      too large to get something without a filter.  ie - district_id
    filter_list : list of strings
      This is the filter list to iterate through.  For example, if you are
      filtering on district_id, this list is all the district_id's you want to 
      iterate through      
    target_dir : absolute path
      Path to the folder you want to save the data to

    Returns
    -------
    None
    '''
    
    base_url = 'https://www.icds-cas.gov.in/a/icds-cas/configurable_reports/data_sources/export/'
    # eventually find a way not to hardcode this
    user, password = get_credentials(r'C:\Users\theism\Documents\Dimagi\Admin\user_info.csv', 'icds')
    i = 1
    for item in filter_list:
        download_url = base_url + ucr_name + '/?format=csv&' + my_filter + '=' + item
        new_file_name = ucr_name + '_' + str(i) + '.csv'
        download_ucr(download_url, user, password, new_file_name, target_dir)
        i += 1
    return

def renumber_files(directory, start_num, basename):
    '''Iterates the number in a filename.  The XXX in cases_XXX.csv would be 
    incremented from a start number in the directory given.  Cases_ is the
    basename in this example.  Start num is the number of the last file that
    exists.  If you have a 101, this will start at 102.'''
    os.chdir(directory)
    files = os.listdir('.')
    num = start_num + 1
    for f in files:
        os.rename(f, basename + str(num) + '.csv')
        num += 1
    print('Renumbering %i files in %s' % (num - start_num, directory))
    return


def is_date(string):
    '''Will analyze a string to assess if it is a date or not.  If yes, returns True, if not, False'''
    try: 
        parse(string)
        return True
    except ValueError:
        return False
 
    
# hat tip: https://www.dataquest.io/blog/pandas-big-data/
def mem_usage(pandas_obj):
    '''Finds memory usage of a pandas dataframe or series
    
    Parameters
    ----------
    pandas_obj : pandas series or dataframe
    
    Returns
    -------
    string of memory usage
    '''
    if isinstance(pandas_obj,pd.DataFrame):
        usage_b = pandas_obj.memory_usage(deep=True).sum()
    else: # we assume if not a df it's a series
        usage_b = pandas_obj.memory_usage(deep=True)
    usage_mb = usage_b / 1024 ** 2 # convert bytes to megabytes
    return "{:03.2f} MB".format(usage_mb)


# hat tip: https://www.dataquest.io/blog/pandas-big-data/
def obj_to_cat(df):
    '''Changes object column types to categoricals if more efficient
    Warning - need to reconvert a column back to numeric dtype to do math on it
    
    Parameters
    ----------
    df : pandas dataframe
    
    Parameters
    ----------
    df : pandas dataframe with object columns turned to categories (where makes sense)
    '''
    df_obj = df.select_dtypes(include=['object'])
    converted_obj = pd.DataFrame()
    for col in df_obj.columns:
        num_unique_values = len(df_obj[col].unique())
        num_total_values = len(df_obj[col])
        if num_unique_values / num_total_values < 0.5:
            converted_obj.loc[:,col] = df_obj[col].astype('category')
        else:
            converted_obj.loc[:,col] = df_obj[col]
    df[converted_obj.columns] = converted_obj
    return df


# combining goodness from: https://www.dataquest.io/blog/pandas-big-data/
def optimize_df_memory(df):
    '''Optimize the memory usage of a dataframe by turning objects into 
    categories and downcasting floats and ints
    
    Parameters
    ----------
    df : pandas dataframe
    
    Parameters
    ----------
    df : memory optimized pandas dataframe
    '''
    logging.info('Optimizing dataframe memory...')
    initial_mem = mem_usage(df)
    types = df.dtypes.unique()    
    # turn objects to categories
    if object in types:
        df = obj_to_cat(df)
    # downcast float
    if float in types:
        df_float = df.select_dtypes(include=['float'])
        conv_float = df_float.apply(pd.to_numeric,downcast='float')
        df[conv_float.columns] = conv_float    
    # downcast int
    if int in types:
        df_int = df.select_dtypes(include=['int'])
        conv_int = df_int.apply(pd.to_numeric,downcast='integer')
        df[conv_int.columns] = conv_int
    end_mem = mem_usage(df)
    logging.info('Dataframe memory optimized from %s to %s' % (initial_mem, end_mem))
    return df