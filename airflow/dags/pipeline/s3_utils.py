from airflow.hooks.S3_hook import S3Hook    
import pandas as pd
import json
import io 

def get_bucket_folders(bucket_name):
    s3, bucket = get_s3_bucket(bucket_name)
    folders = s3.list_keys(bucket, '')
    return folders

def get_file_path(bucket, date):
    """ Depending on the layer, return the path to the S3 file

    Args:
        endpoint (string): name of the folder and file
        bucket (string): layer of the datalake
        date (string): name of the subfolder related to the current run

    Raises:
        Exception: Only 'raw' and 'core' layers are allowed

    Returns:
        string: path to the file
    """
    S3_BUCKET_RAW = "raw"
    S3_BUCKET_CORE = "core"
    if bucket == S3_BUCKET_RAW:
        return date + "/data.json"
    elif bucket == S3_BUCKET_CORE:
        return date + "/data.parquet"
    else:
        raise Exception('Unable to get the file path. Invalid bucket.')

def get_s3_bucket(bucket_name):
    """ Check if a bucket exists and if not, creates and returns it

    Args:
        bucket_name (string): layer of the datalake

    Raises:
        Exception: Only 'raw' and 'core' layers are allowed

    Returns:
        S3Hook, strings: return an hook to S3 and the bucket of the layer
    """
    s3 = S3Hook('loka_aws')
    S3_BUCKET = bucket_name
    if not S3_BUCKET in ('raw', 'core'):
        raise Exception('Invalid bucket name provided.')
    if not s3.check_for_bucket(S3_BUCKET):
        s3.create_bucket(S3_BUCKET)
    return s3, S3_BUCKET

def load_json_to_s3_raw(json_string, date):
    """ Load a json to S3 (raw layer)

    Args:
        json_obj (json): Json object to be uploaded
        endpoint (string): name of the endpoint
    """
    s3, bucket = get_s3_bucket('raw')
    s3.load_string(json_string, get_file_path(bucket, date), 
        bucket_name=bucket, replace=True)

def load_df_from_s3_raw(date):
    """ Load a dataframe from the raw layer

    Args:
        endpoint (string): name of the endpoint

    Returns:
        DataFrame: df with the raw data of endpoint for that run
    """
    s3, bucket_raw = get_s3_bucket('raw')
    if s3.check_for_key(key = get_file_path(bucket_raw, date), bucket_name = bucket_raw):
        raw_file = s3.read_key(key = get_file_path(bucket_raw, date), bucket_name = bucket_raw)
        raw_json = json.loads(raw_file)
        df = pd.json_normalize(raw_json)
    else:
        df = pd.DataFrame()
    return df

def load_df_to_s3_core(df, table_name, context):
    """ Load a dataframe to the core layer as a parquet file

    Args:
        df (DataFrame): df to be loaded
    """
    s3, bucket_core = get_s3_bucket('core')
    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False)
    buffer.seek(0, 0)
    s3.load_file_obj(buffer, get_file_path(bucket_core, table_name + '/' + context['ds']), bucket_name=bucket_core, replace=True)
