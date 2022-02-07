#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import pandas as pd
import boto3
import io
from io import StringIO
import datetime,timedelta

def lambda_handler(event, context):
    today=datetime.date.today()
    s3_file_key=str(today)+ ".csv"
    s3_file_key1=str(today)+"-imdb.csv"
    #s3_file_key = event['Records'][0]['s3']['object']['key'];
    bucket = 'cloudwatchbucket123';
    s3 = boto3.client('s3', aws_access_key_id='AKIATVIL4LQYAO6DOHQI',  aws_secret_access_key='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')
    obj = s3.get_object(Bucket=bucket, Key=s3_file_key)
    obj1 = s3.get_object(Bucket=bucket, Key=s3_file_key1)
    df = pd.read_csv(io.BytesIO(obj['Body'].read()));
    df_imdb = pd.read_csv(io.BytesIO(obj1['Body'].read()));

    service_name = 's3'
    region_name = 'ap-south-1'
    aws_access_key_id = 'AKIATVIL4LQYAO6DOHQI'
    aws_secret_access_key = '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'
    s3_resource = boto3.resource(
        service_name=service_name,
        region_name=region_name,
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key
    )
    bucket='targetetlbucket';
    #df = initial_df[(initial_df.type == "Movie")];
    #df = df.loc[:, ~df.columns.isin(['date_added', 'description', 'duration'])];
    csv_buffer = StringIO()
    df.to_csv(csv_buffer,index=False);
    s3_resource.Object(bucket, s3_file_key).put(Body=csv_buffer.getvalue())

    bucket='targetbucketimdb';
    df_imdb = df_imdb.loc[:, ~df_imdb.columns.isin(['Released','Awards','Poster','imdbID','Production','Website','Response'])];
    csv_buffer = StringIO()
    df_imdb.to_csv(csv_buffer,index=False);
    s3_resource.Object(bucket, s3_file_key1).put(Body=csv_buffer.getvalue())
    
