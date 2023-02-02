from airflow import DAG
import logging as log
import pendulum
from airflow.operators.python import PythonOperator
import requests
from airflow.models import Variable
from airflow.models import TaskInstance

def get_auth_header():
	my_bearer_token = Variable.get("TWITTER_BEARER_TOKEN")
	return {"Authorization": f"Bearer {my_bearer_token}"}

def get_users(ti: TaskInstance, **kwargs):
	users = Variable.get(f"TWITTER_USER_IDS", [], deserialize_json=True)
	user_id = users[0]
	#for user_id in users:
	api_url = f"https://api.twitter.com/2/users/{user_id}?user.fields=public_metrics,profile_image_url,username,description,id"
	request = requests.get(api_url, headers=get_auth_header())
	ti.xcom_push("list_of_users", request)
	return

def get_tweets(ti: TaskInstance, **kwargs):
	tweets = Variable.get(f"TWITTER_TWEET_IDS", [], deserialize_json=True)
	for tweet_id in tweets:
		api_url = f"https://api.twitter.com/2/users/{user_id}?tweet.fields=public_metrics,author_id,text"
		request = requests.get(api_url, headers=get_auth_header())
		tweet_requests.append(request.json())
		ti.xcom_push("list_of_tweets", tweet_requests.json)
	return

def read_and_print_users(ti: TaskInstance, **kwargs):
	users_list = ti.xcom_pull(key="list_of_users", task_ids="get_users")
	logging.info(users_list)
	return

def read_and_print_tweets(ti: TaskInstance, **kwargs):
	tweets_list = ti.xcom_pull(key="list_of_tweets", task_ids="get_tweets")
	logging.info(tweets_list)
	return

with DAG(
	dag_id="twitter_api",
	schedule_interval="0 10 * * *",
	start_date=pendulum.datetime(2023, 1, 1, tz="US/Pacific"),
	catchup=False,
) as dag:
	start_task = PythonOperator(
		task_id="get_users",
		python_callable=get_users,
		provide_context=True)
	first_task = PythonOperator(
		task_id="get_tweets",
		python_callable=get_tweets,
		provide_context=True)
	second_task = PythonOperator(
		task_id="second_task",
		python_callable=read_and_print_users,
		provide_context=True)
	third_task = PythonOperator(
		task_id="third_task",
		python_callable=read_and_print_tweets,
		provide_context=True)


start_task >> first_task >> second_task >> third_task
