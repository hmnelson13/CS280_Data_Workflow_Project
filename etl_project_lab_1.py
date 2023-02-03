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

def get_twitter_api_data_task(ti: TaskInstance, **kwargs):
	users = Variable.get(f"TWITTER_USER_IDS", [], deserialize_json=True)
	user_requests = []
	for user_id in users:
		api_url = f"https://api.twitter.com/2/users/{user_id}?user.fields=public_metrics,profile_image_url,username,description,id"
		request = requests.get(api_url, headers=get_auth_header())
		log.info(request)
		user_requests.append(request.json())
	ti.xcom_push("list_of_users", user_requests)

	tweets = Variable.get(f"TWITTER_TWEET_IDS", [], deserialize_json=True)
	tweet_requests = []
	for tweet_id in tweets:
		api_url = f"https://api.twitter.com/2/tweets/{tweet_id}?tweet.fields=public_metrics,author_id,text"
		request = requests.get(api_url, headers=get_auth_header())
		log.info(request)
		tweet_requests.append(request.json())
	ti.xcom_push("list_of_tweets", tweet_requests)
	return

def transform_twitter_api_data_func(ti: TaskInstance, **kwargs):
        users_list = ti.xcom_pull(key="list_of_users", task_ids="get_users")
        tweets_list = ti.xcom_pull(key="list_of_tweets", task_ids="get_tweets")

        df_users = pd.read_json(users_list)
        df_tweets = pd.read_json(tweets_list)
        return


with DAG(
	dag_id="twitter_api",
	schedule_interval="0 10 * * *",
	start_date=pendulum.datetime(2023, 1, 1, tz="US/Pacific"),
	catchup=False,
) as dag:
	extract = PythonOperator(
		task_id="extract",
		python_callable=get_twitter_api_data_task,
		provide_context=True)
	transform = PythonOperator(
		task_id="transform",
		python_callable=transform_twitter_api_data_func,
		provide_context=True)


extract >> transform
