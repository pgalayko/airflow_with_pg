from airflow.models.baseoperator import BaseOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


class CustomSqlOperator(BaseOperator):
    def __init__(self,
                 sql: str,
                 out_schema: bool,
                 out_table: str,
                 pg_conn: str = 'dwh_pg_conn',
                 **kwargs) -> None:
        super().__init__(**kwargs)
        self.sql = sql
        self.out_schema = out_schema
        self.out_table = out_table
        self.pg_conn = pg_conn

    # Оператор выполнения кастомного sql-запроса.
    # Получаем схему и название таргет таблицы, sql запрос, который необходимо выполнить
    # Передаем в запрос create table as с указанием таргета и сам sql запрос

    def execute(self, context):
        hash_value = context['ti'].xcom_pull(
            task_ids='get_load_parameters', key='hash')
        create_target = f"create table {self.out_schema}.{self.out_table}_{hash_value} as select * from ({self.sql.format(hash=hash_value)}) t"

        hook_dwh = PostgresHook(postgres_conn_id=self.pg_conn)
        hook_dwh.run(create_target)
