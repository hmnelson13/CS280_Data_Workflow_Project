from airflow import DAG
import logging as log                                                                                          
import pendulum
from airflow.operators.python import PythonOperator
import requests
from airflow.models import Variable
from airflow.models import TaskInstance


def transform_twitter_api_data_func(ti: TaskInstance, **kwargs):
	users_list = ti.xcom_pull(key="list_of_users", task_ids="get_users")
	tweets_list = ti.xcom_pull(key="list_of_tweets", task_ids="get_tweets")
	
	df_users = pd.read_json(users_list)
	df_tweets = pd.read_json(tweets_list)
        return



with DAG(
        dag_id="transform",
        schedule_interval="0 10 * * *",
        start_date=pendulum.datetime(2023, 1, 1, tz="US/Pacific"),
        catchup=False,
) as dag:
        start_task = PythonOperator(
                task_id="data_func",
                python_callable=transform_twitter_api_data_func,
                provide_context=True)

start_task
