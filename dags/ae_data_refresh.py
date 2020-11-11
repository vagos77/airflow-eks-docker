import logging
from datetime import datetime, timezone

import airflow
from airflow import DAG
from airflow.contrib.operators.snowflake_operator import SnowflakeOperator
from airflow.operators.python_operator import PythonOperator

# from scripts import create_pivot_table as cpt
# from scripts import custom_snowflake as db_sf
# from scripts import drop_transient_tables as dtt
# from scripts import link_table as lt

# currentdir = os.path.dirname(os.path.realpath(__file__))
# parentdir = os.path.dirname(currentdir)
# sys.path.append(parentdir)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

args = {"owner": "vagos", "start_date": airflow.utils.dates.days_ago(2)}
snowflake_connection = {
    'account': 'altusgroupargus.eu-west-1',
    'user': 'FIXME',
    'password': 'FIXME',
    'warehouse': 'LOAD_WH_VAGOS',
    'database': 'AIRFLOW_PARALLEL'
}

warehouse = snowflake_connection['warehouse']
database = snowflake_connection['database']

parallelism = 5

dag = DAG(
    dag_id="ae-data-refresh", default_args=args, schedule_interval=None
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

replacement_dict = {'{SOURCE}': 'AE'}

with dag:
    """ 
        - Loads AE data from S3
        - Creates necessary resources such as database/schema/tables
        - Builds Data/Business Vaults
        - Pivots KPIs
        - Build Link Table in Business Vault
    """

    # waiting_for_ae_data = S3KeySensor(task_id="waiting-for-ae-data",
    #                                   aws_conn_id="my_s3_conn",
    #                                   bucket_key="s3://voyanta-glue/snowflake/json/ae/refresh_ae.txt",
    #                                   poke_interval=5)

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
        task_id='create-data-vault-tables-ae',
        sql=extract_sql_commands('scripts/sql/data_vault/dv_create_tables.sql'),
        snowflake_conn_id='snowflake_conn',
        warehouse=warehouse,
        database=database,
        schema='DATA_VAULT',
    )

    load_data_ae_coa = SnowflakeOperator(
        task_id='load-data-ae-coa',
        sql=extract_sql_commands('scripts/sql/data_vault/dv_ae_load_coa.sql'),
        snowflake_conn_id='snowflake_conn',
        warehouse=warehouse,
        database=database,
        schema='DATA_VAULT',
    )

    load_data_ae = SnowflakeOperator(
        task_id='load-data-ae',
        sql=extract_sql_commands('scripts/sql/data_vault/dv_ae_load_data.sql'),
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

    pivot_kpi = PythonOperator(
        task_id='pivot-kpi-ae',
        python_callable=cpt.create_pivot_table,
        op_kwargs={
            'connection_dict': snowflake_connection,
            'source_database': snowflake_connection['database'],
            'source_schema': 'DATA_VAULT',
            'source_table': 'SAT_AE_PROPERTY_KPI',
            'destination_schema': 'DATA_VAULT',
            'destination_table': 'TRANSIENT_AE_PROPERTY_KPI_PIVOTED',
            'create_view': False,
            'base_columns': 'MD5_HUB_AE_PROPERTY, CURRENCY_BASIS, IS_ASSURED, ACCOUNT_ID,VALUATION_DATE_VALUE, UNIT_OF_MEASURE, DATE, LDTS',
            'pivot_columns': 'LINE_ITEM_TYPE_NAME',
            'exclude_columns': 'HASH_DIFF, LINE_ITEM_TYPE_ID, RSRC'
        }
    )

    build_business_vault_dims = SnowflakeOperator(
        task_id='build-bv-dimensions',
        sql=extract_sql_commands('scripts/sql/business_vault/bv_build_dimensions_ae.sql', replacement_dict),
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
            'source': 'AE'
        }
    )

    # Dependencies up to this point
    create_database >> create_database_resources >> create_data_vault_tables
    create_data_vault_tables >> load_data_ae_coa >> drop_transient_tables
    create_data_vault_tables >> load_data_ae >> load_data_xs >> pivot_kpi
    pivot_kpi >> build_business_vault_dims >> drop_transient_tables

    # Parallelise SQL commands to create facts
    ae_sql_commands = extract_sql_commands('scripts/sql/business_vault/bv_build_facts_ae.sql', replacement_dict)
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
        pivot_kpi >> build_business_vault_facts >> build_link_table

    # Dependencies after parallel SQL commands
    build_link_table >> drop_transient_tables
