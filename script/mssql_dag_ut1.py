from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.mssql_operator import MsSqlOperator
# from airflow.hooks.oracle_hook import OracleHook
from airflow.utils.dates import days_ago

args = {
    'owner': 'xyz',
    'start_date': days_ago(2),
}

# dag = DAG('example_xcom', schedule_interval="@once", default_args=args, tags=['example'])
# 
# 
# def puller(**kwargs):
#     ti = kwargs['ti']
#     # get value_1
#     pulled_value_1 = ti.xcom_pull(task_ids='data')
#     print("VALUE IN PULLER : ", pulled_value_1)
# 
# 
# def get_data_from_oracle(**kwargs):
#     oracle_hook = OracleHook(oracle_conn_id=kwargs['oracle_conn_id'])
#     return oracle_hook.get_records(sql=kwargs['sql'])
# 
# push = PythonOperator(
#     task_id='data',
#     # op_kwargs={'oracle_conn_id': 'oracle_conn_id', 'sql': 'SELECT * FROM CUSTOMERS'},
#     op_kwargs={'oracle_conn_id': 'oracle_default', 'sql': 'SELECT count(*) FROM ods.FND_COLUMNS'},
#     
#     provide_context=True,
#     python_callable=get_data_from_oracle,
#     dag=dag,
# )
# 
# pull = PythonOperator(
#     task_id='pullee',
#     dag=dag,
#     python_callable=puller,
#     provide_context=True,
# )
# 
# 
# push >> pull
# 

dag = DAG("sql_proc_0", "Testing running of SQL procedures",
          schedule_interval = "@once", 
          catchup = False,
          default_args=args, 
          )
#          start_date = datetime(2021, 1, 1))

# [dbo].[LoadData] is the name of the procedure
# sql_command = """ 
# EXECUTE [dbo].[LoadData] 
# """

sql_command = """ 
select * FROM [odpdev].[INT].[INT_PNL_Revenue_Cost_A]
"""
# select * FROM [odpdev].[INT].[INT_PNL_Revenue_Cost_A]
# select * FROM [odpdev].[INT].[INT_Daily_Revenue_F]

task = MsSqlOperator(task_id = 'run_test_proc', mssql_conn_id = 'mssql_default',
                     sql = sql_command, dag = dag, database = 'odpdev')
                     
