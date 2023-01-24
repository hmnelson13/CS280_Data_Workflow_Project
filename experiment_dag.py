from airflow import DAG
import logging as log
import pendulum
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.bash import BashOperator


def second_task_function():
    print("Printing by python!")
    return

with DAG(
    dag_id="my_first_cs280_dag",
    schedule_interval="0 * * * *",
    start_date=pendulum.datetime(2023, 1, 1, tz="US/Pacific"),
    catchup=False,
) as dag:
    start_task_1 = DummyOperator(task_id="start_task_1")
    first_task_1 = BashOperator(task_id="first_task_1", bash_command='echo "Hello Bash Command!"')
    second_task_1 = PythonOperator(task_id="second_task", python_callable=second_task_function)
    end_task_1 = DummyOperator(task_id="end_task_1")

start_task_1 >> [first_task_1 >> second_task_1]
second_task_1 >> end_task_1
