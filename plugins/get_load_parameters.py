import hashlib

from airflow.models.baseoperator import BaseOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


class LoadParameters(BaseOperator):
    def __init__(self, sources: str,  database: str = 'dwh', pg_conn: str = 'dwh_pg_conn', **kwargs) -> None:
        super().__init__(**kwargs)
        self.sources = sources
        self.database = database
        self.pg_conn = pg_conn
    
    # Функция захвата последней даты изменения источника
    def get_max_moddate(self, source: str, airflow_pg_conn: str = 'airflow_pg_conn') -> str:
        sql = "select coalesce(max(moddate), '1900-01-01') from public.source_moddate_log where source_name = '{}'"
        hook = PostgresHook(postgres_conn_id=airflow_pg_conn)
        last_moddate = str(hook.get_first(sql.format(source))[0])
        return last_moddate


    def get_hash_from_run_id(self, context):
        run_id = context['run_id']
        hash_object = hashlib.md5(f'{run_id}'.encode()).hexdigest()
        return hash_object


    def execute(self, context):
        for source_dict in self.sources:
            if  not source_dict['dict_flg']:
                lmd = self.get_max_moddate(source_dict['source_name'])
                context['ti'].xcom_push(key=f'{source_dict["source_name"]}_lmd', value=lmd)
        
        context['ti'].xcom_push(key='hash', value=self.get_hash_from_run_id(context))
