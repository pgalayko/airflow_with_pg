from datetime import datetime, timedelta
import hashlib

from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models.baseoperator import chain

from get_load_parameters import LoadParameters
from snapshot_operator import SnapshotOperator
from custom_sql_operator import CustomSqlOperator
from load_info_operator import LoadInfoOperator


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
    load_parameters = LoadParameters(
        task_id='get_load_parameters',
        sources=[{'source_name': 'sales.salesorderheader', 'dict_flg': False}, 
                 {'source_name': 'sales.customer', 'dict_flg': True},
                 {'source_name': 'sales.store', 'dict_flg': True},
                 {'source_name': 'person.adress', 'dict_flg': True},
                 ]
    )

    sn_operator_salesorderheader = SnapshotOperator(
        task_id='sn_salesorderheader',
        source_schema='sales',
        source_table='salesorderheader',
        column_list=['salesorderid', 'customerid', 'salespersonid', 'shiptoaddressid', 'orderdate', 'shipdate', 'duedate', 'status', 'subtotal'],
        dict_flg=False,
        stg_schema='af_dm_stg',
        target_table=f't_salesorderheader'
    )

    custom_sql_proc_salesorder = CustomSqlOperator(
        task_id='custom_sql_salesorderheader',
        out_schema='af_dm_stg',
        out_table='t_salesorderheader_2',
        sql=""" select s.salesorderid 
                      ,s.customerid 
                      ,s.salespersonid 
                      ,s.shiptoaddressid
                      ,cast(s.orderdate as date) 
                      ,cast(s.shipdate as date)
                      ,cast(s.duedate as date)
                      ,case 
                          when s.status = 1 then 'In progress'
                          when s.status = 2 then 'Approved'
                          when s.status = 3 then 'Backordered'
                          when s.status = 4 then 'Rejected'
                          when s.status = 5 then 'Shipped'
                          when s.status = 6 then 'Canceled'
                          else 'Unknown'
                      end as status
                      ,s.subtotal
                from af_dm_stg.t_salesorderheader_{hash} s
      """
    )

    sn_operator_customer = SnapshotOperator(
        task_id='sn_customer',
        source_schema='sales',
        source_table='customer',
        column_list=['customerid', 'storeid'],
        dict_flg=True,
        stg_schema='af_dm_stg',
        target_table=f't_customer'
    )

    sn_operator_store = SnapshotOperator(
        task_id='sn_store',
        source_schema='sales',
        source_table='store',
        column_list=['businessentityid', 'name'],
        dict_flg=True,
        stg_schema='af_dm_stg',
        target_table=f't_store'
    )

    sn_operator_address = SnapshotOperator(
        task_id='sn_address',
        source_schema='person',
        source_table='address',
        column_list=['addressid', 'addressline1', 'city'],
        dict_flg=True,
        stg_schema='af_dm_stg',
        target_table=f't_address'
    )

    custom_sql_proc_sales_info = CustomSqlOperator(
        task_id='custom_sql_sales_info',
        out_schema='af_dm_stg',
        out_table='t_sales_info',
        sql=""" select s.salesorderid 
                      ,s.customerid 
                      ,c.storeid 
                      ,s.salespersonid 
                      ,s.shiptoaddressid
                      ,concat(a.addressline1, ', ', a.city) as ship_to_address
                      ,st."name" as store_name
                      ,cast(s.orderdate as date) 
                      ,cast(s.shipdate as date)
                      ,cast(s.duedate as date)
                      ,s.status
                      ,s.subtotal
                from af_dm_stg.t_salesorderheader_2_{hash} s 
                left join af_dm_stg.t_customer_{hash} c on s.customerid = c.customerid
                left join af_dm_stg.t_store_{hash} st on c.storeid = st.businessentityid
                left join af_dm_stg.t_address_{hash} a on s.shiptoaddressid = a.addressid"""
    )

    insert_data = LoadInfoOperator(
        task_id='load_sales_info',
        source_schema='af_dm_stg',
        source_table='t_sales_info',
        target_schema='af_dm_mart',
        target_table='sales_info'
    )

    chain(load_parameters, 
          sn_operator_salesorderheader,
          custom_sql_proc_salesorder, 
          [sn_operator_customer, sn_operator_store, sn_operator_address], 
          custom_sql_proc_sales_info,
          insert_data)
