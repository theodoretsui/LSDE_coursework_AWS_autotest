# -*- coding: utf-8 -*-
'''
@Time    : 2021-12-05
@Author  : Xu_Wentao

'''
import boto3
import csv
import json

sqs = boto3.resource('sqs')
sqs_results = sqs.get_queue_by_name(QueueName='wordfreq-results')

# get all messages in results MQ
result_message_file = input('Enter result message file: ')
result_message_csv = result_message_file + '.csv'
result_message_file = result_message_file + '.txt'
headers = ['StartTime', 'TextName', 'Status', 'StatusMsg','SentTimestamp', 'SenderId']
rows = []

while True:
    sqs_result_message_list = sqs_results.receive_messages(AttributeNames = ['All'])
    with open(result_message_file, 'a') as file:
        try:
            result_msg = sqs_result_message_list[0].body
            result_msg_attr = sqs_result_message_list[0].attributes
            file.write(result_msg + '\n'*2)
            result_msg_json = json.loads(result_msg)
            row = (\
                result_msg_json['Job']['StartedAt'],\
                result_msg_json['Job']['Key'],\
                result_msg_json['Status'],\
                result_msg_json['StatusMessage'],\
                result_msg_attr['SentTimestamp'],\
                result_msg_attr['SenderId'])
            rows.append(row)
            print(row)
            sqs_result_message_list[0].delete()
        except:
            pass
    if not sqs_result_message_list:
        break
# write important msg in csv

with open(result_message_csv, 'w', newline='') as f:
    try:
        writer = csv.writer(f)
        writer.writerow(headers)
        writer.writerows(rows)
    except:
        pass
