# -*- coding: utf-8 -*-
"""
Created on Sat Jan 27 10:50:49 2018

@author: theism
"""

import os
import pandas as pd
import gen_func as gf
import re
import logging

data_dir = r'C:\Users\theism\Downloads\[DA] Post Natal Care\[DA] Post Natal Care'
data_regex = re.compile(r'Forms_\d\d\d.csv')
output_df = pd.DataFrame()
output_df = output_df.fillna('')
output_name = 'combined_file.csv'
file_list = gf.data_file_list(data_dir, data_regex)
gf.start_logging(data_dir)

for data_file in file_list:
    # get data
    logging.info('going through %' % data_file)
    input_df = pd.read_csv(os.path.join(data_dir, data_file), infer_datetime_format=True, low_memory=False)
    output_df = pd.concat([output_df, input_df], axis=1)
    
output_df.to_csv(os.path.join(data_dir, output_name))
logging.info('all files combined, output saved to directory')
    
    
