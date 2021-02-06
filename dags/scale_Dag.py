from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.mysql_operator import MySqlOperator
from airflow.operators.email_operator import EmailOperator
from airflow.operators.dummy_operator import DummyOperator

from module import preprocessing, kpi_to_parquet

yesterday_date = datetime.strftime(datetime.now() - timedelta(1), '%Y-%m-%d')

default_args = {
    'owner': 'Airflow',
    'start_date': datetime(2021, 1, 23),
    'retries': 1,
    'retry_delay': timedelta(seconds=5)
}

dag = DAG('scale_Dag', default_args=default_args, schedule_interval='0 6 * * *', template_searchpath=['/usr/local/airflow/sql_files'], catchup=False)

start_dag = DummyOperator(task_id='start.dag', 
  retries=1, depends_on_past=False, dag=dag)

end_dag = DummyOperator(task_id='end.dag', 
  retries=1, depends_on_past=False, trigger_rule="all_done", dag=dag)


t1=BashOperator(task_id='check_metrics', bash_command='shasum ~/campaign_files_airflow/metrics.csv', retries=2, retry_delay=timedelta(seconds=15), dag=dag )

t2=PythonOperator(task_id='columnar_format', python_callable=preprocessing, dag=dag)

t3=BashOperator(task_id='check_metrics_columnar', bash_command='shasum ~/campaign_files_airflow/metrics_columnar.csv', retries=2, retry_delay=timedelta(seconds=15), dag=dag )

t4=PythonOperator(task_id='kpi_to_parquet', python_callable=kpi_to_parquet, dag=dag)


start_dag >> t1 >> t2 >> t3 >> t4 >> end_dag