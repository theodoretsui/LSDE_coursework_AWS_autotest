# -*- coding: utf-8 -*-
'''
@Time    : 2021-12-05
@Author  : Xu_Wentao

'''
import pandas as pd
import os

filename = input('filename: ')
filename = filename + '.csv'

dtype_dict = {
    'StartTime':'datetime64', 
    'SentTimestamp': 'datetime64[ms]'}
result_msg_df = pd.read_csv(filename, header = 0)
result_msg_df = result_msg_df.astype(dtype_dict)

failure_df = result_msg_df[result_msg_df.Status == 'failure']
failure_count = failure_df.Status.count()

success_df = result_msg_df[result_msg_df.Status == 'success']
success_dup = success_df.Status.count() - success_df.drop_duplicates(subset = ['TextName']).Status.count()

work_instance = success_df.drop_duplicates(subset=['SenderId']).Status.count()

print(
    'Total running time: ' +
    str(pd.Timedelta(max(result_msg_df.SentTimestamp) - min(result_msg_df.StartTime)).seconds) + 
    f' Seconds. (found {success_dup} duplicate processes!)({work_instance} instances worked)({failure_count} failure)')
os.system('PAUSE')