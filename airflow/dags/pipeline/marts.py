from airflow.hooks.postgres_hook import PostgresHook
from pipeline.ingestion import get_date_range
import datetime

def model_vehicles_op_stats(**context):
    psql = PostgresHook('loka_dwh')
    pipeline_mode = context['params']['pipeline_mode']
    if pipeline_mode == 'full_refresh':
        psql.run(f"CALL marts.model_vehicles_op_stats(null, null);", autocommit=True)
    else :
        start_date, end_date = get_date_range(context)
        psql.run(f"CALL marts.model_vehicles_op_stats('{start_date}', '{end_date}');", autocommit=True)
