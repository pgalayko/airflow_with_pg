from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

default_args = {
    'owner': 'pgalayko',
    'retries': 0,
    'retry_delay': timedelta(minutes=2)
}

with DAG(
    dag_id='test_pg_operator',
    default_args=default_args,
    description='pass',
    start_date=datetime(2023, 7, 15, 6, 00),
    schedule_interval='@once'
) as dag:
    # get_max_loaded_pk_value = PostgresOperator(
    #     task_id='get_max_pk',
    #     postgres_conn_id='dwh_pg_conn',
    #     database='dwh',
    #     sql="""
    #         select coalesce(max(salesorderid), 0) from af_dm_stg.salesorderheader;
    #     """
    # )

    def get_data_from_sql_statement(pk_column, schema_name, table_name, database='dwh'):
        request = f'select coalesce(max({pk_column}), 0) from {schema_name}.{table_name};'
        pg_hook = PostgresHook(postgres_conn_id='dwh_pg_conn', database=database)
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        cursor.execute(request)
        result = cursor.fetchone()[0]
        print(f'Max PK value = {result}')
        return {'source': f'{schema_name}.{table_name}','max_loaded_pk_value': str(result)}

    def print_info(info):
        return print(info)
    
    print_max_value = PythonOperator(
        task_id='print_max_pk',
        python_callable=get_data_from_sql_statement,
        op_kwargs={'pk_column': 'salesorderid', 'schema_name': 'af_dm_stg', 'table_name': 'salesorderheader'}
    )
    
    print_max_value
    # get_max_loaded_pk_value >> print_max_value
    

