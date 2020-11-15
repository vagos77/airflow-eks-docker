import logging
import os
import sys
import json
from datetime import datetime, timezone, timedelta

import airflow
from airflow import DAG
from airflow.contrib.operators.snowflake_operator import SnowflakeOperator
from airflow.operators.python_operator import PythonOperator

# Add parent level dir in path to allow for import
sys.path.append(
    os.path.abspath(os.path.join(os.path.dirname(__file__), os.path.pardir)))

# Import from parent level - DO NOT MOVE
from scripts import create_pivot_table as cpt
from scripts import custom_snowflake as db_sf
from scripts import drop_transient_tables as dtt
from scripts import link_table as lt

from custom_aws import secrets_manager as sm

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2020, 1, 1),
    "email": ["support@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1)
}

# Retrieve Snowflake connection details from Secrets Manager
secrets_manager = sm.SecretsManager()
# Convert string to dictionary which is what Snowflake connector expects
snowflake_connection = json.loads(secrets_manager.get_secret('bi/connections/snowflake_admin'))

# FIXME the correct warehouse and database should be used for each org
warehouse = 'LOAD_WH_VAGOS'
database = 'AIRFLOW_PARALLEL'

snowflake_connection['warehouse'] = warehouse
snowflake_connection['database'] = database

parallelism = 5

dag = DAG(
    dag_id="at-data-refresh", default_args=args, schedule_interval=None
)

current_timestamp_utc = datetime.now(timezone.utc)


def extract_sql_commands(script, replacement_dict=None):
    logging.info('Extracting SQL from {}'.format(script))

    with open(script) as f:
        sql_script = f.read()

    if replacement_dict:
        # Replace placeholders
        for k in replacement_dict:
            sql_script = sql_script.replace(k, replacement_dict[k])

    sql_commands = sql_script.split(';')

    sql_commands_clean = []
    for s in sql_commands:
        # Avoid empty lines
        if len(s) > 1:
            sql_commands_clean.append(s)
    f.close()
    # logging.info('Clean SQL commands: {}'.format(['{};'.format(s) for s in sql_commands_clean]))

    # Return list with ; at the end of each query
    return ['{};'.format(s) for s in sql_commands_clean]


def build_bv_link_table(connection_dictionary):
    # Init Snowflake object
    snowflake_db = db_sf.SnowflakeDatabase(connection_dictionary)
    database_name = snowflake_connection['database']
    link_table_query = lt.generate_link_table_query(snowflake_db, database_name)
    snowflake_db.execute_query(link_table_query)


# Yield successive n-sized chunks from l.
def divide_chunks(l, n):
    # looping till length l
    for i in range(0, len(l), n):
        yield l[i:i + n]


create_database_query = [
    """CREATE DATABASE IF NOT EXISTS {DB};""".format(DB=database),
    """CREATE SCHEMA IF NOT EXISTS DATA_VAULT;""",
    """CREATE SCHEMA IF NOT EXISTS BUSINESS_VAULT;""",
]

variables_query = [
    """ALTER SESSION SET TIMEZONE = 'UTC';""",
    """SET LoadDTS = \'{TIMESTAMP}\';""".format(TIMESTAMP=current_timestamp_utc),
    # Temporary variables required where source files are not currently providing data
    # Change these as required for testing, e.g. incrementing version number
    # FIXME
    """SET S3folder = 'V6.5';""",
    """SET AEPropertyLastModifiedBy = 'user@example.com';""",
    """SET AEPropertyLastModifiedDate = \'{TIMESTAMP}\';""".format(TIMESTAMP=current_timestamp_utc)
]

replacement_dict = {'{SOURCE}': 'AT'}

with dag:
    """
        - Loads AT data from S3
        - Creates necessary resources such as database/schema/tables
        - Builds Data/Business Vaults
        - Pivots facts and dimensions data
        - Build Link Table in Business Vault
    """

    create_database = SnowflakeOperator(
        task_id='create-database',
        sql=create_database_query,
        snowflake_conn_id='snowflake_conn',
        warehouse=warehouse,
    )

    create_database_resources = SnowflakeOperator(
        task_id='create-database-resources',
        sql=extract_sql_commands('scripts/sql/resources/database_resources.sql'),
        snowflake_conn_id='snowflake_conn',
        warehouse=warehouse,
        database=database,
        schema='DATA_VAULT',
    )

    create_data_vault_tables = SnowflakeOperator(
        task_id='create-data-vault-tables',
        sql=extract_sql_commands('scripts/sql/data_vault/dv_create_tables.sql'),
        snowflake_conn_id='snowflake_conn',
        warehouse=warehouse,
        database=database,
        schema='DATA_VAULT',
    )

    create_transient_tables = SnowflakeOperator(
        task_id='create-transient-tables',
        sql=variables_query + extract_sql_commands('scripts/sql/data_vault/dv_at_create_transient_tables.sql'),
        snowflake_conn_id='snowflake_conn',
        warehouse=warehouse,
        database=database,
        schema='DATA_VAULT',
    )

    load_data_xs = SnowflakeOperator(
        task_id='load-data-cross',
        sql=extract_sql_commands('scripts/sql/data_vault/dv_xs_load_data.sql', replacement_dict),
        snowflake_conn_id='snowflake_conn',
        warehouse=warehouse,
        database=database,
        schema='DATA_VAULT',
    )

    build_business_vault_dims = SnowflakeOperator(
        task_id='build-bv-dimensions',
        sql=extract_sql_commands('scripts/sql/business_vault/bv_build_dimensions_at.sql'),
        snowflake_conn_id='snowflake_conn',
        warehouse=warehouse,
        database=database,
        schema='BUSINESS_VAULT',
    )

    pivot_at_characteristic = PythonOperator(
        task_id='pivot-at-characteristic',
        python_callable=cpt.create_pivot_table,
        op_kwargs={
            'connection_dict': snowflake_connection,
            'source_database': snowflake_connection['database'],
            'source_schema': 'DATA_VAULT',
            'source_table': 'TRANSIENT_AT_DIMENSION_CHARACTERISTIC',
            'destination_schema': 'DATA_VAULT',
            'destination_table': 'TRANSIENT_AT_DIMENSION_CHARACTERISTIC_PIVOTED',
            'create_view': False,
            'base_columns': 'MD5_HUB_AT_ENTITY,DIMENSION',
            'pivot_columns': 'LINE_ITEM',
            'type_column': 'TYPE'
        }
    )

    pivot_at_dkpi = PythonOperator(
        task_id='pivot-at-dkpi',
        python_callable=cpt.create_pivot_table,
        op_kwargs={
            'connection_dict': snowflake_connection,
            'source_database': snowflake_connection['database'],
            'source_schema': 'DATA_VAULT',
            'source_table': 'TRANSIENT_AT_DIMENSION_KPI',
            'destination_schema': 'BUSINESS_VAULT',
            'destination_table': 'DIMENSION_KPI_AT',
            'create_view': False,
            'base_columns': 'PK_DIMENSION_KPI_AT,FK_DIMENSION_SCENARIO_AT,FK_DIMENSION_ENTITY_AT,FK_DIMENSION_DATE,DIMENSION',
            'pivot_columns': 'LINE_ITEM',
            'exclude_columns': 'TAG,CATEGORY',
            'type_column': 'TYPE'
        }
    )

    pivot_at_dflow = PythonOperator(
        task_id='pivot-at-dflow',
        python_callable=cpt.create_pivot_table,
        op_kwargs={
            'connection_dict': snowflake_connection,
            'source_database': snowflake_connection['database'],
            'source_schema': 'DATA_VAULT',
            'source_table': 'TRANSIENT_AT_DIMENSION_FLOW',
            'destination_schema': 'BUSINESS_VAULT',
            'destination_table': 'DIMENSION_FLOW_AT',
            'create_view': False,
            'base_columns': 'PK_DIMENSION_FLOW_AT,FK_DIMENSION_SCENARIO_AT,FK_DIMENSION_ENTITY_AT,FK_DIMENSION_DATE,DIMENSION',
            'pivot_columns': 'LINE_ITEM',
            'exclude_columns': 'TAG,CATEGORY',
            'type_column': 'TYPE'
        }
    )

    pivot_at_fkpi = PythonOperator(
        task_id='pivot-at-fkpi',
        python_callable=cpt.create_pivot_table,
        op_kwargs={
            'connection_dict': snowflake_connection,
            'source_database': snowflake_connection['database'],
            'source_schema': 'DATA_VAULT',
            'source_table': 'TRANSIENT_AT_FACT_KPI',
            'destination_schema': 'BUSINESS_VAULT',
            'destination_table': 'FACT_KPI_AT',
            'create_view': False,
            'base_columns': 'PK_FACT_KPI_AT,FK_DIMENSION_SCENARIO_AT,FK_DIMENSION_ENTITY_AT,FK_DIMENSION_DATE,FK_DIMENSION_CURRENCY,DIMENSION',
            'pivot_columns': 'LINE_ITEM',
            'exclude_columns': 'TAG,CATEGORY',
            'type_column': 'TYPE'
        }
    )

    build_business_vault_dims2 = SnowflakeOperator(
        task_id='build-bv-dimensions2',
        sql=extract_sql_commands('scripts/sql/business_vault/bv_build_dimensions_after_pivot_at.sql'),
        snowflake_conn_id='snowflake_conn',
        warehouse=warehouse,
        database=database,
        schema='BUSINESS_VAULT',
    )

    build_business_vault_dims_cross = SnowflakeOperator(
        task_id='build-bv-dimensions-xs',
        sql=extract_sql_commands('scripts/sql/business_vault/bv_build_dimensions_xs.sql', replacement_dict),
        snowflake_conn_id='snowflake_conn',
        warehouse=warehouse,
        database=database,
        schema='BUSINESS_VAULT',
    )

    build_link_table = PythonOperator(
        task_id='build-bv-link-table',
        python_callable=build_bv_link_table,
        op_kwargs={'connection_dictionary': snowflake_connection}
    )

    drop_transient_tables = PythonOperator(
        task_id='drop-transient-tables',
        python_callable=dtt.drop_transient_tables,
        op_kwargs={
            'connection_dict': snowflake_connection,
            'source': 'AT'
        }
    )

    # Dependencies up to this point
    create_database >> create_database_resources >> create_data_vault_tables >> create_transient_tables

    # Parallelise SQL commands to load the data vault tables
    at_sql_commands = extract_sql_commands('scripts/sql/data_vault/dv_at_load_data.sql')
    items_per_task = round(len(at_sql_commands) / parallelism)
    sql_chunks = [c for c in divide_chunks(at_sql_commands, items_per_task)]

    for i, chunk in enumerate(sql_chunks):
        task_id = 'load-data-at-{}'.format(i)
        load_data_at = SnowflakeOperator(
            task_id=task_id,
            sql=variables_query + chunk,
            # Combine variables with query chunk. Some of the queries require the variables to be set first
            snowflake_conn_id='snowflake_conn',
            warehouse=warehouse,
            database=database,
            schema='DATA_VAULT',
        )
        create_transient_tables >> load_data_at >> load_data_xs

    # Dependencies after parallel SQL commands
    load_data_xs >> build_business_vault_dims

    # Parallelise SQL commands to create facts
    ae_sql_commands = extract_sql_commands('scripts/sql/business_vault/bv_build_facts_at.sql')
    items_per_task = round(len(ae_sql_commands) / parallelism)
    sql_chunks = [c for c in divide_chunks(ae_sql_commands, items_per_task)]

    for i, chunk in enumerate(sql_chunks):
        task_id = 'build-business-vault-facts-{}'.format(i)
        build_business_vault_facts = SnowflakeOperator(
            task_id=task_id,
            sql=chunk,
            snowflake_conn_id='snowflake_conn',
            warehouse=warehouse,
            database=database,
            schema='BUSINESS_VAULT',
        )
        load_data_xs >> build_business_vault_facts >> pivot_at_fkpi

    pivot_at_fkpi >> build_link_table >> drop_transient_tables
    build_business_vault_dims >> pivot_at_characteristic >> build_business_vault_dims2 >> build_business_vault_dims_cross >> drop_transient_tables
    build_business_vault_dims >> pivot_at_dkpi >> drop_transient_tables
    build_business_vault_dims >> pivot_at_dflow >> drop_transient_tables
