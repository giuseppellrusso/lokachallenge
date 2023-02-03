from datetime import timedelta, datetime as dt

from airflow.models import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator

import logging 

from pipeline.db_utils import setup_db
from pipeline.ingestion import extract_raw_data
from pipeline.core import process_operating_periods, process_vehicle_events, model_vehicles
from pipeline.marts import model_vehicles_op_stats

args = {
    'owner': 'Giuseppe',
    'start_date': days_ago(2),
}

dag = DAG(
    dag_id = 'loka',
    default_args = args,
    schedule_interval = None,
    params = {
        'pipeline_mode': 'reload', #  Possible values: reload - full_refresh - incremental
        'reload_from': '2019-06-01',
        'reload_to': '2019-06-01',
        'drop_tables': False
    },
    dagrun_timeout = timedelta(minutes = 60),
    tags=['loka', 'task', 'http', 's3', 'postgres']
)

def check_parameters(**context):
    """ This function checks if the parameters passed to the DAG are valid.
        In particular,
        - pipeline_mode should be either 'reload', 'incremental' or 'full_refresh', 
            denoting the way in which the pipeline will handle data
        - reload_from and reload_to are needed in case of full_refresh to denote
            the time period in which you want to reload data
        - drop_tables is a flag, set to true if you want to erase the content of the
            database before starting the pipeline.
    """
    logging.info("Checking validity of the parameters for the DAG run ...")
    params = context['params']
    if params['pipeline_mode'] not in ('reload', 'full_refresh', 'incremental'):
        raise Exception('Invalid pipeline mode')
    if params['pipeline_mode'] == 'reload':
        date_format = "%Y-%m-%d"
        try:
            bool(dt.strptime(params['reload_from'], date_format))
            bool(dt.strptime(params['reload_to'], date_format))
        except:
            raise Exception('Invalid format for reload dates')
    if not isinstance(params['drop_tables'], bool):
        raise Exception('drop_tables is not a boolean')
    logging.info('Parameters are valid!')

"""
    This task will check the correctness of the parameters
    passed to the dag
"""
check_parameters_task = PythonOperator(
    task_id = 'check_parameters',
    dag = dag,
    python_callable = check_parameters
)

"""
    This task will launch the basics DDLs to be sure all
    needed tables are present in the database.
"""
setup_db_task = PythonOperator(
    task_id = 'setup_db',
    dag = dag,
    python_callable = setup_db
)

"""
    This task is responsible for extracting raw data in json
    format and store them into the raw layer of the datalake
"""
extract_task = PythonOperator(
    task_id = 'extract_raw_data',
    dag = dag,
    python_callable = extract_raw_data
)

"""
    This task is responsible for modeling the operating_periods
    dimension.
"""
process_op_task = PythonOperator(
    task_id = 'process_operating_periods',
    dag = dag,
    python_callable = process_operating_periods
)


"""
    This task is responsible for modeling the vehicles events
    fact table.
"""
process_ve_task = PythonOperator(
    task_id = 'process_vehicle_events',
    dag = dag,
    python_callable = process_vehicle_events
)


"""
    This task is responsible for modeling the vehicles
    dimension.
"""
model_vehicles_task = PythonOperator(
    task_id = 'model_vehicles',
    dag = dag,
    python_callable = model_vehicles
)

"""
    This task is responsible for building the mart layer 
    of the datawarehouse.
"""
model_vehicles_op_task = PythonOperator(
    task_id = 'model_vehicles_op_stats',
    dag = dag,
    python_callable = model_vehicles_op_stats
)

check_parameters_task >> [ setup_db_task, extract_task ] >> process_op_task >> process_ve_task >> model_vehicles_task >> model_vehicles_op_task

if __name__ == "__main__":
    dag.cli()