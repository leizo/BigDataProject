from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from lib.spotify_fetcher import fetch_data_from_spotify
from lib.lastfm_fetcher import fetch_data_from_lastfm
from lib.raw_to_fmt_spotify import convert_raw_to_formatted

with DAG(
       'my_first_dag',
       default_args={
           'depends_on_past': False,
           'email': ['airflow@example.com'],
           'email_on_failure': False,
           'email_on_retry': False,
           'retries': 1,
           'retry_delay': timedelta(seconds=15),
       },
       description='A first DAG',
       schedule_interval=None,
       start_date=datetime(2021, 1, 1),
       catchup=False,
       tags=['example'],
) as dag:
   dag.doc_md = """
       This is my first DAG in airflow.
       I can write documentation in Markdown here with **bold text** or __bold text__.
   """


   def launch_task(**kwargs):
       print("Hello Airflow - This is Task with task_number:", kwargs['task_number'])
       print("kwargs['dag_run']", kwargs["dag_run"].execution_date)


   source_to_raw_1 = PythonOperator(
       task_id='fetch_data_from_lastfm',
       python_callable=fetch_data_from_lastfm,
       provide_context=True,
       op_kwargs={'task_number': 'task1'}
   )

   source_to_raw_2 = PythonOperator(
       task_id='fetch_data_from_spotify',
       python_callable=fetch_data_from_spotify,
       provide_context=True,
       op_kwargs={'task_number': 'task3'}
   )

   raw_to_formatted_1 = PythonOperator(
       task_id='format_data_from_lastfm',
       python_callable=launch_task,
       provide_context=True,
       op_kwargs={'task_number': 'task2'}
   )

   raw_to_formatted_2 = PythonOperator(
       task_id='format_data_from_spotify',
       python_callable=convert_raw_to_formatted,
       provide_context=True,
       op_kwargs={'task_number': 'task4'}
   )

   produce_usage = PythonOperator(
       task_id='produce_usage',
       python_callable=launch_task,
       provide_context=True,
       op_kwargs={'task_number': 'task5'}
   )

   index_to_elastic = PythonOperator(
       task_id='index_to_elastic',
       python_callable=launch_task,
       provide_context=True,
       op_kwargs={'task_number': 'task6'}
   )

   source_to_raw_1.set_downstream(raw_to_formatted_1)
   source_to_raw_2.set_downstream(raw_to_formatted_2)
   raw_to_formatted_1.set_downstream(produce_usage)
   raw_to_formatted_2.set_downstream(produce_usage)
   produce_usage.set_downstream(index_to_elastic)
