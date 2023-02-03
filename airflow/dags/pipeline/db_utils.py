import logging 
from airflow.hooks.postgres_hook import PostgresHook

def get_df_from_query(query):
    """ Run a query on the db and returns a df with the result

    Args:
        query (string): Query to be run

    Returns:
        DataFrame: df with the result of the query
    """
    psql = PostgresHook('loka_dwh')
    df = psql.get_pandas_df(query)
    return df
  
def get_df_from_table(table_schema, table_name, condition = True):
    """ Convert a db table to a dataframe

    Args:
        table_schema (string): schema of the table
        table_name (string): name of the table

    Returns:
        DataFrame: df with the table content
    """
    query = f"SELECT * FROM {table_schema}.{table_name} WHERE {condition}"
    return get_df_from_query(query)

def full_refresh(table_schema, table_name, tuple_lists):
    """ Perform a full refresh of a table, thus truncating the current
        content and inserting the passed tuples

    Args:
        table_schema (string): schema of the table
        table_name (string): name of the table
        tuple_lists (List): list of rows to be inserted in the table
    """
    psql = PostgresHook('loka_dwh')
    psql.run(f"TRUNCATE TABLE {table_schema}.{table_name};", autocommit=True)
    psql.insert_rows(
        table = f"core.{table_name}",
        rows = tuple_lists
    ) 

def upsert(table_schema, table_name, tuple_lists, replace_index, target_fields):
    """ Perform an upsert on a target table based on some new rows, a conflict
        condition and a list of target fields to update in case of conflict

    Args:
        table_schema (string): schema of the table
        table_name (string): name of the table
        tuple_lists (List): list of rows to be inserted in the table
        replace_index (str or list of string): index of the table
        target_fields (list of string): list of fields to update in case of conflict
    """
    psql = PostgresHook('loka_dwh')
    psql.insert_rows(
        table = f"{table_schema}.{table_name}",
        rows = tuple_lists,
        replace = True,
        replace_index = replace_index, 
        target_fields = target_fields
    )

def update_partition(df, partition_schema, target_schema, target_table):
    """ Update tables with partition based on the date fields.
        This function basically takes all the possible partitions from an 
        input dataframe and for each of them it create a new partition on 
        the target table. If such a partition already existed, it will get
        overwritten. 

    Args:
        df (DataFrame): input dataframe (to be inserted in the table)
        partition_schema (string): schema where to create partition tables
        target_schema (string): schema of the table to update
        target_table (string): name of the table to update
    """
    psql = PostgresHook('loka_dwh')
    dates = list(df.event_date.astype(str).unique())
    for date in dates:
        partition_name = f'{target_table}_date_' + date.replace('-', '_')
        partition_df = df[df.event_date == date]
        partition_new_rows = list(partition_df.itertuples(index=False, name=None))
        # Dropping eventual already existing partition and creating a new one based on the date, then inserting the data
        psql.run(f"DROP TABLE IF EXISTS {partition_schema}.{partition_name};")
        psql.run(f"CREATE TABLE {partition_schema}.{partition_name} PARTITION OF {target_schema}.{target_table} FOR VALUES IN ('{date}');")
        psql.insert_rows(
            table = f"{partition_schema}.{partition_name}",
            rows = partition_new_rows
        )   

def run_statements_from_sql_file(sqlfile):
    """ Run a .sql file

    Args:
        sqlfile (file): Path to the .sql file containing sql statements
    """
    fd = open(sqlfile, 'r')
    sqlFile = fd.read()
    psql = PostgresHook('loka_dwh')
    psql.run(sqlFile, autocommit=True)
    fd.close()    

def setup_db(**context):
    """ Setup the postgres database
        In particular, it creates the necessary schema and launch
        the necessary DDLs to correctly execute the pipeline.
        If in the dag parameters it is specified, this function also
        cleanup everything and re-instatiate it.
    """
    logging.info('Setting up the db...')
    drop_tables = context['params']['drop_tables']
    if drop_tables :
        run_statements_from_sql_file('dags/queries/drop_tables.sql')
    run_statements_from_sql_file('dags/queries/ddl.sql')
