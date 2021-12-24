# -*- coding: utf-8 -*-
'''
@Time    : 2021-12-05
@Author  : Xu_Wentao

'''
import boto3
import os

# Get the service resource
sqs = boto3.resource('sqs')
s3 = boto3.resource('s3')
ec2 = boto3.resource('ec2')
dynamoDB = boto3.resource('dynamodb')

bucket = s3.Bucket('af-wordfreq-qf21808')
sqs_jobs = sqs.get_queue_by_name(QueueName='wordfreq-jobs')
sqs_results = sqs.get_queue_by_name(QueueName='wordfreq-results')

# test connection
# print(bucket.name) # if print af-wordfreq-qf21808 then works well

# delete all files in s3
objects = bucket.objects.all()
objects_to_delete = [{'Key': o.key} for o in objects if o.key.endswith('.txt')]
if len(objects_to_delete):
    s3.meta.client.delete_objects(Bucket='af-wordfreq-qf21808', Delete={'Objects': objects_to_delete})

# get all messages in results MQ ******** USE getResultMsg.py
# result_message_file = input('Enter result message file: ')
# result_message_file = result_message_file + '.txt'
# result_message_file = 'null.txt'
# while True:
#     sqs_result_message_list = sqs_results.receive_messages(AttributeNames = ['All'])
#     with open(result_message_file, 'a') as file:
#         try:
#             file.write(str(sqs_result_message_list[0].body) + '\n'*2)
#             sqs_result_message_list[0].delete()
#         except:
#             pass
    
#     if not sqs_result_message_list:
#         break

# upload files to s3
test_file_dir = './testFile/'
for filename in os.listdir(test_file_dir):
    bucket.upload_file(test_file_dir + filename, filename)
    print(f'Have sent file: {filename}')

