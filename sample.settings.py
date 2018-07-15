import os

# Useful paths assuming relative path structure of
# ROOT/Code/this file
# ROOT/Data/some_data_dir/data_files
# ROOT/Results/some_output_dir/output_files

ROOT_FOLDER = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))

# Use join for cross platform support
DATA_DIR = os.path.join(ROOT_FOLDER, 'Data')
OUTPUT_DIR = os.path.join(ROOT_FOLDER, 'Results')

HASH_FILE = 'files.hash'
HDF_KEY = 'table'
