from airflow.models.baseoperator import BaseOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


class LoadInfoOperator(BaseOperator):
    def __init__(self, 
                 source_schema: str,
                 source_table: str,
                 target_schema: str,
                 target_table: str,
                 pg_conn: str = 'dwh_pg_conn',
                 **kwargs) -> None:
        super().__init__(**kwargs)
        self.source_schema = source_schema
        self.source_table = source_table
        self.target_schema = target_schema
        self.target_table = target_table
        self.pg_conn = pg_conn


    def execute(self, context):
        hash_value = context['ti'].xcom_pull(
            task_ids='get_load_parameters', key='hash')
        insert_into_target = f"insert into {self.target_schema}.{self.target_table} select * from {self.source_schema}.{self.source_table}_{hash_value} t"

        hook_dwh = PostgresHook(postgres_conn_id=self.pg_conn)
        hook_dwh.run(insert_into_target)
        
        


