import imp
from json.tool import main
import pandas as pd
import boto3
import os

bucket_name = 'assignment-simplexity'

def download_s3(bucket, key_file_path):
    file_path = './battery_energy.csv'
    s3 = boto3.client('s3')
    s3.download_file(Bucket=bucket, 
                    Key= key_file_path,
                    Filename= file_path)
    return file_path #'./battery_energy.csv'

def transformation(file_path):
    result_file_path = './battery_energy_day.csv'
    battery_energy = pd.read_csv(file_path)
    battery_energy['Date'] = pd.to_datetime(battery_energy['DateTime']).dt.date
    new_battery_energy = battery_energy.groupby(['Date','Site']).agg({'battery_current' : 'mean', 'battery_voltage' : 'mean', 'battery_power' : 'mean', 'battery_energy' : 'sum'})

    new_battery_energy.to_csv(result_file_path)
    return result_file_path #'./battery_energy_day.csv'

def upload_file(result_file_path, bucket_name):

    result_to_upload = result_file_path
    bucket_tp_upload = bucket_name
    
#    s3 = boto3.client('s3')
#    s3.upload_file(bucket_tp_upload, result_to_upload)
    s3 = boto3.client('s3')
    s3.upload_file(result_file_path, bucket_name,'output_file/battery_energy_day.csv')
    return 1

def clearlocalfile(files:list):
    
    for path in files:

        os.remove(path)

file_path_0 = download_s3(bucket_name, "input_file/battery_energy.csv")
trans_path = transformation(file_path_0)
upload_file = upload_file(trans_path, bucket_name)
clearlocalfile([file_path_0, trans_path]) 
