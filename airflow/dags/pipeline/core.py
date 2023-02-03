from pipeline.s3_utils import load_df_from_s3_raw, get_bucket_folders, load_df_to_s3_core
from pipeline.db_utils import upsert, update_partition, get_df_from_table, full_refresh
from pipeline.ingestion import get_date_range
from pipeline.file_utils import file_date
import numpy as np 
import pandas as pd
import logging
import datetime as dt 


def clean_raw_data(df):
    """ Clean the data dataframe by renaming columns, filling it with proper Null values
        and applying some simple logic conditions.

    Args:
        df (DataFrame): df containing raw data

    Returns:
        df: cleaned dataframe
    """
    # General renaming to make fields already more readable
    df = df.rename(columns={
            'on': 'event_entity_type', 
            'at': 'event_at', 
            'data.id': 'entity_id',
            'data.location.lat' : 'latitude', 
            'data.location.lng' : 'longitude', 
            'data.location.at' : 'location_collected_at', 
            'data.start' : 'entity_start', 
            'data.finish' : 'entity_end'}
        )
    
    df = df.replace({np.nan:None})

    # Based on some simple data profiling we can assert that the timestamp of the update event (when location
    # is collected) and the location_timestamp are the same. For this reason we can filter out this duplicated
    # column. An alert should be triggered if a row not respecting this condition is found
    if not ((df.event_at == df.location_collected_at) | (df.location_collected_at.isna())).all():
        logging.warning('Found some rows with different event_at and location_collected_at timestamps.')
    df = df.drop('location_collected_at', axis=1)

    return df

def pipeline_operating_periods(df):
    """ For operating periods we simply have to get the information from the creation events 
        that we are interested in and do some casting in order to get to a dimension table.
        Since the only event we have for operating periods is the creation, no real aggregation is needed
        to define to the dimension.

    Args:
        df (DataFrame): raw data dataframe

    Returns:
        DataFrame: operating periods dimension dataframe
    """
    operating_periods = df[df.event_entity_type == 'operating_period']
    # Consistency check on the event types for operating periods: only creation is allowed
    if (operating_periods.event != 'create').any():
        logging.warning('Found new event for operating periods.')
    # We are only interested in the id, creation, start and end timestamp of the operating periods
    operating_periods = operating_periods[["entity_id", "organization_id", "event_at", "entity_start", "entity_end"]]
    operating_periods = operating_periods.rename(columns = {
        'entity_id' : 'operating_period_id',
        'entity_start': 'started_at',
        'entity_end': 'ended_at',
        'event_at': 'created_at'
        })
    # Casting the timestamps
    operating_periods['started_at'] = operating_periods.started_at.astype('datetime64[ns]')
    operating_periods['ended_at'] = operating_periods.ended_at.astype('datetime64[ns]')
    operating_periods['created_at'] = operating_periods.created_at.astype('datetime64[ns]')
    operating_periods['_last_updated_at'] = dt.datetime.now()

    return operating_periods

def check_vehicle_events_consistency(df):
    """ This function performs consistency checks
        When dealing with event data, my experience showed me that it can very often happen to retrieve
        incosistent events. From the data profiling done in the given dataset this seems not to be the case,
        however I wanted to show I would check the consistency as a general rule.
        For vehicle events, it is essential that the first event is the registering of the vehicle,
        and the last one is the deregister.
        This function actually checks the first and last events and return a list of vehicles id which 
        do not fit the reasoning before: either they have an event prior to registering, or one after the
        deregistering.

    Returns:
        List: list of unconsistent vehicles id
    """
    # Getting vehicles whose first event is not a register event
    unconsistent_events_prior_register = df[(df["event_index"] == 1.0) & (df.event != 'register')]
    unconsistent_vehicles_list= list(unconsistent_events_prior_register.vehicle_id.unique())

    # Getting vehicles whose last event is not a deregister event
    last_event_by_vehicle = df.loc[df.groupby("vehicle_id")["event_index"].idxmax()]
    unconsistent_events_after_deregister = last_event_by_vehicle[last_event_by_vehicle.event != 'deregister']

    unconsistent_vehicles_list.extend(list(unconsistent_events_after_deregister.vehicle_id.unique()))
    return unconsistent_vehicles_list

def get_operating_period_id(op_df, event_timestamp):
    """ Return the operating period associated to a timestamp.
        Strong assumption: there is no overlap between operating periods (verified)

    Args:
        op_df (DataFrame): operating periods
        event_timestamp (timestamp): timestamp of the event

    Returns:
        str: operating_period_id occurring at the moment of the timestamp
    """
    # To make this work we indexed the dataframe with an IndexInterval containing start and end of the 
    # operating period. In this function we will compare the timestamp with the index of this df.

    # If the timestamp is not in the range of any of the operating periods, return None
    if (op_df.index.contains(event_timestamp) == False).all():
        return None
    # If one of the index interval contains the timestamp, return the operating period id of the column connected
    else:
        return op_df.iloc[op_df.index.get_loc(event_timestamp)]['operating_period_id']

def flag_first_last_update(df, partition, column_prefix = ''):
    """ This function ranks the events by a partition (either by vehicle, or by <vehicle, operating period>) 
        and flag the first and last event

    Args:
        df (DataFrame): vehicles events df
        partition (List): list of columns to partition by the dataframe
        column_prefix (str, optional): prefix for the columns to add. Defaults to ''.

    Returns:
        DataFrame: vehicles events with first/last flag columns
    """
    # Getting ascending rank
    df['update_index_asc'] = df.groupby(partition)["event_at"].rank(method = 'first', ascending = True)
    # Getting descending rank
    df['update_index_desc'] = df.groupby(partition)["event_at"].rank(method = 'first', ascending = False)
    # First update is the first one in ascending rank
    df[f'is_first{column_prefix}_update'] = df.apply(lambda x : True if x.event == 'update' and x.update_index_asc == 1 else False, axis = 1)
    # Last update is the first one in descending rank
    df[f'is_last{column_prefix}_update'] = df.apply(lambda x : True if x.event == 'update' and x.update_index_desc == 1 else False, axis = 1)
    df = df.drop(['update_index_asc', 'update_index_desc'], axis = 1)
    return df

def pipeline_vehicles_events(df, op_df):
    """ Getting the vehicles events and building a fact table by enriching it with:
        - operating period occurring during the event (see later docs for more info);
        - event rank by vehicle;
        - flags which denotes the first/last update for the vehicle in general, and 
            for each operating period.

    Args:
        df (DataFrame): raw data df
        op_df (DataFrame): operating periods df

    Returns:
        DataFrame: vehicles events dataframe
    """
    # Getting only vehicles events
    vehicles_events = df[df.event_entity_type == 'vehicle']
    # Renaming and casting
    vehicles_events = vehicles_events.rename(columns = {
            'entity_id' : 'vehicle_id'
        })
    vehicles_events['event_at'] = vehicles_events.event_at.astype('datetime64[ns]')

    # Before dropping the operating periods dedicated columns, we do a consistency check
    if not ((vehicles_events.entity_start.isna()) & (vehicles_events.entity_end.isna())).all():
        logging.warning('Found some vehicles events rows with start and end date.')
    vehicles_events = vehicles_events.drop(['entity_start', 'entity_end', 'event_entity_type'], axis=1)

    # Adding a ranking for the events of each vehicle, based on the ascending timestamp.
    # This will be useful for future processing.
    vehicles_events['event_index'] = vehicles_events.groupby("vehicle_id")["event_at"].rank(method = 'first', ascending = True)

    # Consistency check
    # We decide to remove from the dataset all vehicles with inconsistent data. Another option
    # could have been to remove only events which were not consistent, but depending on the volume
    # of data it could be a better option to simply remove possible dirty parts of the dataset.
    unconsistent_vehicles_ids = check_vehicle_events_consistency(vehicles_events)
    vehicles_events = vehicles_events[~vehicles_events.vehicle_id.isin(unconsistent_vehicles_ids)]

    # Join with Operating Period
    # Since it is not described the relation between vehicles and operating periods, I need to do an assumption
    # I see two options:
    # 1) (chosen) There is an n:n relation between vehicle and operating periods. Depending on the timestamp of the
    #   update event, a vehicle could be associated to a specific operating period (or even to none of them).
    #   In such an interpretation, operating periods are just a timestamp range where to analyze performances.
    # 2) We connect vehicles to 0:1 operating periods (assuming there is no overlap between them) by using 
    #   the registering timestamp of the vehicle.
    # Here we perform a join assuming that there is no overlap between operating periods. This means that every 
    # timestamps can fit at most to one of the ranges of the operating periods.
    # If there was such an overlap situation (but this is  not the case), we would have used as a tie-breaker 
    # the operating period id in ascending order.
    op_df.index = pd.IntervalIndex.from_arrays(op_df['started_at'],op_df['ended_at'],closed='both')
    vehicles_events['operating_period_id'] = vehicles_events['event_at'].apply(lambda x : get_operating_period_id(op_df, x))

    # Adding flags to detect first and last update in general and by operating period
    vehicles_events = flag_first_last_update(vehicles_events, ['vehicle_id', 'event'])
    vehicles_events = flag_first_last_update(vehicles_events, ['vehicle_id', 'event', 'operating_period_id'], '_op')
    
    # Some helpers columns
    vehicles_events['event_date'] = pd.to_datetime(vehicles_events['event_at']).apply(lambda x: x.date()).astype('datetime64[ns]')
    vehicles_events['_last_updated_at'] = dt.datetime.now()
    return vehicles_events


def pipeline_vehicles(df):
    """ Creating the vehicles model. What we do here is to take basic information about the vehicles (id, organization) and
        details about registration and first/last location update timestamps and positions. These timestamps will be very 
        useful to stakeholders for analysis purposes.

    Args:
        df (DataFrame): vehicles events dataframe

    Returns:
        DataFrame: vehicles dataframe
    """
    if df.empty:
        return df
    vehicles_registrations = df[df.event == 'register'][['vehicle_id', 'organization_id', 'event_at']].rename(columns = {'event_at' : 'registered_at'})
    vehicles_deregistrations = df[df.event == 'deregister'][['vehicle_id', 'event_at']].rename(columns = {'event_at' : 'deregistered_at'})
    vehicles_first_locations = df[df.is_first_update][["vehicle_id", "event_at", "latitude", "longitude"]].rename(columns = {'event_at': 'first_location_update_at', 'latitude': 'first_location_latitude', 'longitude': 'first_location_longitude'})
    vehicles_last_locations = df[df.is_last_update][["vehicle_id", "event_at", "latitude", "longitude"]].rename(columns = {'event_at': 'last_location_update_at', 'latitude': 'last_location_latitude', 'longitude': 'last_location_longitude'})
     
    vehicles = vehicles_registrations.merge(vehicles_deregistrations).merge(vehicles_first_locations).merge(vehicles_last_locations)
    vehicles['_last_updated_at'] = dt.datetime.now()
    return vehicles

def get_raw_data(context):
    """ Get raw data from S3 bucket, depending on the pipeline mode.
        If it's full refresh, all files in the raw bucket will be considered.
        If it's reload/incremental, only files within the range of dates provided
            will be considered.

    Returns:
        DataFrame: dataframe contaning raw data
    """
    logging.info("Loading raw data from S3...")
    pipeline_mode = context['params']['pipeline_mode']
    df = pd.DataFrame()

    if pipeline_mode == 'full_refresh':
        files = get_bucket_folders('raw')
        dates = [file_date(file) for file in files]
        # For every date we append to an empty dataframe the df coming from the file related to the date
        for date in dates:
            df = df.append(load_df_from_s3_raw(str(date)))
        logging.info("Full refresh done: all raw files have been loaded")
    else :
        startdate, enddate = get_date_range(context)
        date_range = [date.strftime("%Y-%m-%d") for date in pd.date_range(startdate, enddate, freq='d')]
        files = get_bucket_folders('raw')
        # Filtering the date range with only dates for which a folder is present in the bucket
        filtered_date_range = [date for date in date_range if any(file_date(file) == date for file in files)]
        for date in filtered_date_range:
            df = df.append(load_df_from_s3_raw(date))
        logging.info(f"Pipeline is in mode {pipeline_mode}: files from {startdate} to {enddate} correctly loaded.")
    return df

def load_data_to_core(context, df, table_name, column_id):
    """ Load data to the core layer. In particular, loading it into S3 as a parquet
        and in the datawarehouse as a core table. 

    Args:
        df (DataFrame): dataframe to be loaded
        table_name (string): name of the table
        column_id (string): name of the id of the column (for upserts)
    """
    pipeline_mode = context['params']['pipeline_mode']
    df_rows = list(df.itertuples(index=False, name=None))
    if pipeline_mode == 'full_refresh':
        # If full refresh, truncate + insert
        full_refresh('core', table_name, df_rows)
        load_df_to_s3_core(df, table_name, context)
    else:
        # If incremental/reload, upsert
        upsert('core', table_name, df_rows, column_id, list(df.columns))
        load_df_to_s3_core(df, table_name, context)

def process_operating_periods(**context):
    """ Creating the operating periods dimension model and loading it to core layer
    """
    df = get_raw_data(context)
    if df.empty:
        return
    df = clean_raw_data(df)
    operating_periods = pipeline_operating_periods(df)
    load_data_to_core(context, operating_periods, 'operating_periods', 'operating_period_id')

def process_vehicle_events(**context):
    """ Creating the vehicles events fact model and loading it to core
    """
    df = get_raw_data(context)
    if df.empty:
        return
    df = clean_raw_data(df)
    operating_periods = get_df_from_table('core', 'operating_periods')
    vehicles_events = pipeline_vehicles_events(df, operating_periods)
    # Since the table is partitioned by date, we need to update partitions
    update_partition(vehicles_events, 'events_partitions', 'core', 'vehicles_events')
    load_df_to_s3_core(df, 'vehicles_events', context)

def model_vehicles(**context):
    """ Creating the vehicle dimension model and loading it to the core layer
    """
    pipeline_mode = context['params']['pipeline_mode']
    if pipeline_mode == 'full_refresh':
        condition = True
    else :
        start_date, end_date = get_date_range(context)
        condition = f"event_date >= '{start_date}' and event_date <= '{end_date}'"
    # We will create the dimension based on the events from the fact table of the 
    # period of time specified by the pipeline mode
    vehicles_events = get_df_from_table('core', 'vehicles_events', condition)
    vehicles = pipeline_vehicles(vehicles_events)
    load_data_to_core(context, vehicles, 'vehicles', 'vehicle_id')