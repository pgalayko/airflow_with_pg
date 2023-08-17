from airflow.models.baseoperator import BaseOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


class SnapshotOperator(BaseOperator):
    def __init__(self, 
                 source_schema: str,
                 source_table: str,
                 column_list,
                 stg_schema: str,
                 target_table: str,
                 where_clause: str = None,
                 dict_flg: bool = False, 
                 pg_conn: str = 'dwh_pg_conn',
                 **kwargs) -> None:
        super().__init__(**kwargs)
        self.source_schema = source_schema
        self.source_table = source_table
        self.column_list = column_list
        self.where_clause = where_clause
        self.dict_flg = dict_flg
        self.stg_schema = stg_schema
        self.target_table = target_table
        self.pg_conn = pg_conn


# Снепшот.
# На вход: схема источника, название таблицы источника, флаг словаря, stg_schema, условие where
# Получаем hash, получаем lmd для источника (не справочника).
# Если источник является справочником, грузим полностью

    def execute(self, context):
        hash_value = context['ti'].xcom_pull(
            task_ids='get_load_parameters', key='hash')
        
        if not self.dict_flg:
            lmd = context['ti'].xcom_pull(
                task_ids='get_load_parameters', key=f'{self.source_schema}.{self.source_table}_lmd')
        create_temp_table = f"create table {self.stg_schema}.{self.target_table}_{hash_value} as select {','.join(self.column_list)} " + \
                            f"from {self.source_schema}.{self.source_table} where "
        get_new_lmd_sql = f"select max(cast(modifieddate as date)) from {self.source_schema}.{self.source_table}"
        if self.where_clause and not self.dict_flg:
            create_temp_table += f"{self.where_clause} and modifieddate > '{lmd}'"
        elif not self.where_clause and not self.dict_flg:
            create_temp_table += f"modifieddate > '{lmd}'"
        elif self.where_clause and self.dict_flg:
            create_temp_table += f'{self.where_clause}'
        else:
            create_temp_table += '1 = 1'

        hook_dwh = PostgresHook(postgres_conn_id=self.pg_conn)
        hook_dwh.run(create_temp_table)  # Запускаем скрипт создания временной таблицы
        new_lmd = hook_dwh.get_first(get_new_lmd_sql)[0]

        # Загружаем новую дату изменения в лог-таблицу, если она != существующей записи
        if not self.dict_flg:
            if str(new_lmd) != str(lmd):
                log_lmd_sql = "insert into public.source_moddate_log (source_name, moddate) values ('{}', '{}')"
                hook_airflow = PostgresHook(postgres_conn_id='airflow_pg_conn')
                hook_airflow.run(log_lmd_sql.format(f'{self.source_schema}.{self.source_table}', new_lmd))
            else:
                print('Новая дата загрузки источника не была добавлена в лог, так как совпадает в предыдущим значением')
        
        


