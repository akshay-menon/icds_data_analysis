# -*- coding: utf-8 -*-
"""
Created on Mon Feb 05 07:25:12 2018

@author: theism
"""

'formid',
'form.owner_id',
'form.is_weight_instructions',  # did the user ask to see the weighing instructions
'form.zscore_wfa',
'form.zscore_wfa_prev',
'form.zscore_grading_wfa',  # if(/data/zscore_wfa = '', '', if(/data/zscore_wfa < -3, 'red', if(/data/zscore_wfa < -2, 'yellow', if(/data/zscore_wfa < 2, 'green', 'white'))))
'form.weight_change',     # int((/data/weight_child - /data/weight_prev) * 100) div 100
'form.weight_change_status',  # if(/data/weight_change > 0, 'increased', if(/data/weight_change < 0, 'decreased', 'no_prev'))
'form.count',  # number of times a growth monitoring form filled out
'form.sex',
'form.dob',
'form.age_in_days',
'form.date_first_detection',    # first time zscore < -3
'form.case_child_health_0.case.@case_id',
'form.case_child_health_0.case.@date_modified',
'form.negative_growth_time',   # if(/data/weight_child < /data/weight_prev, double(now()), /data/prev_negative_growth_time)
'form.prev_negative_growth_time', 
'form.static_growth_time',      # if(/data/weight_child = /data/weight_prev, double(now()), /data/prev_static_growth_time)
'form.prev_static_growth_time',

# get all growth forms
# add columns to see if weighed in given timeframe after birth
# add columns to see if weighed in combination of timeframes
# add column if malnourished
# groupby caseid and create new df, counting added columns
# stats on weight increase or not
# are users using the weight instructions
# is it always the same user selecting to see the instructions?
# is there a difference in who gets weighed based on gender?
# measure days between weighings
# see how long have been wfa zscore < -3
# avrg number of times see growth mon form filled out - by age?