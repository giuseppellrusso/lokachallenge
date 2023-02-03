from pipeline.s3_utils import load_json_to_s3_raw
from pipeline.db_utils import get_df_from_query
from pipeline.file_utils import is_json, file_date
from datetime import timedelta, datetime
import logging
import os
import pandas as pd
pd.set_option('display.max_columns', None)

def get_last_update_date():
    """ Get the last processed date on the fact table

    Returns:
        date: the last event date from the vehicles_events fact table
    """
    df = get_df_from_query(f"SELECT MAX(event_date) as last_update_date FROM core.vehicles_events")       
    last_update_date = df.last_update_date.iloc[0]
    return last_update_date

def get_date_range(context):
    """ This function returns the date range to be used for
        calling the api.

    Returns:
        date: start and end date for the call api
    """


    params = context['params']
    pipeline_mode = params['pipeline_mode']
    if pipeline_mode == 'reload':
        """ 
            For reload mode, we will use the date range provided in the parameters.
        """
        forced_reload_start = params['reload_from']
        forced_reload_end = params['reload_to'] or default_update_date
        logging.info(f"Pipeline is in mode FORCE_RELOAD from {forced_reload_start} to {forced_reload_end}")
        return forced_reload_start, forced_reload_end

    if pipeline_mode == 'incremental':
        """
            For incremental mode, each DAG run we aim at updating the data 
            of the db up to the day before the execution date.
            For instance, if we run the DAG on the 31st of January, we expect
            the db to be updated up to the 30th of January.
            The default_update_date field will store the day before the 
            execution.
        """
        execution_date = datetime.strptime(context['ds'], '%Y-%m-%d').date()
        default_update_date = execution_date - timedelta(days = 1)
        last_update_date = get_last_update_date()
        # If there is data in the table, we want to collect data since the first following day
        if last_update_date:
            last_update_date += timedelta(days = 1)
        # Otherwise we will just collect the last day data
        start_date = last_update_date or default_update_date
        end_date = default_update_date
        logging.info(f"Pipeline is in mode INCREMENTAL from {start_date} to {end_date}")
        return start_date, end_date
    
    return 

def extract_raw_data(**context):
    """ INGESTION STEP 
        This function is responsible for ingesting data into the datalake.
        Sources could be any, as an example:
        - some API endpoints;
        - an SFTP folder;
        - an external S3 Bucket.

        In the task an S3 Bucket is mentioned but I was not able to reach it. For this
        reason I decided to create a data/ folder inside the project where to store the
        "external files".
        The idea of ingestinon would be the same. It would only change the way in which 
        we get to the .json file.

        As an output of the function, we will have the data in the raw bucket of S3.
    """
    ingestion_folder = os.listdir('data/')

    pipeline_mode = context['params']['pipeline_mode']

    daily_data = []
    if pipeline_mode == 'full_refresh':
        logging.info("Pipeline is in mode FULL_REFRESH. All data will be fetched")
        """ 
            If pipeline is in full refresh mode we will get all the data in the ingestion folder
        """
        daily_data = [file for file in ingestion_folder if is_json(file)]    
    else : 
        """
            If pipeline is in incremental/reload mode we will cosider only the files 
            associated to a date between a start and end date.
        """
        startdate, enddate = get_date_range(context)
        date_range = pd.date_range(startdate, enddate, freq='d')
        daily_data = [file for file in ingestion_folder if is_json(file) and file_date(file) in date_range]

    """
        I create an empty dataframe with a pre-defined schema where to place the result of the ingestion
        This will help us validating the data
    """
    result_df = pd.DataFrame({
                    'event': pd.Series(dtype='str'),
                    'on': pd.Series(dtype='int'),
                    'at': pd.Series(dtype='datetime64[ns]'),
                    'data': pd.Series(dtype='str'),
                    'organization_id': pd.Series(dtype='str')
                })
    
    # Reading the files as a dataframe
    for file in daily_data:
        df = pd.read_json('data/' + file, lines=True)
        result_df = pd.concat([result_df, df])
    
    """
        Based on the date field (at timestamp in the json) we will split
        the data into folders inside of the datalake.
    """
    result_df['date'] = result_df['at'].astype('datetime64[ns]').dt.date
    dates = list(result_df.date.unique())

    for date in dates:
        logging.info(f"Logging data for date {date} into S3, raw bucket")
        partition = result_df[result_df.date == date]
        partition = partition.drop(['date'], axis = 1)
        json_string = partition.to_json(orient = 'records')
        load_json_to_s3_raw(json_string, str(date))