from airflow import DAG
import logging as log
import pendulum
from airflow.operators.python import PythonOperator
import requests
from airflow.models import Variable
from airflow.models import TaskInstance
import pandas as pd

def get_auth_header():
	my_bearer_token = Variable.get("TWITTER_BEARER_TOKEN")
	return {"Authorization": f"Bearer {my_bearer_token}"}

def get_twitter_api_data_task(ti: TaskInstance, **kwargs):
	users = Variable.get(f"TWITTER_USER_IDS", [], deserialize_json=True)
	user_requests = []
	for user_id in users:
		api_url = f"https://api.twitter.com/2/users/{user_id}?user.fields=public_metrics,profile_image_url,username,description,id"
		request = requests.get(api_url, headers=get_auth_header())
		user_requests.append(request.json())
	ti.xcom_push("list_of_users", user_requests)
	log.info(user_requests)

	tweets = Variable.get(f"TWITTER_TWEET_IDS", [], deserialize_json=True)
	tweet_requests = []
	for tweet_id in tweets:
		api_url = f"https://api.twitter.com/2/tweets/{tweet_id}?tweet.fields=public_metrics,author_id,text"
		request = requests.get(api_url, headers=get_auth_header())
		tweet_requests.append(request.json())
	ti.xcom_push("list_of_tweets", tweet_requests)
	log.info(tweet_requests)
	return

def transform_twitter_api_data_func(ti: TaskInstance, **kwargs):
        users_list = ti.xcom_pull(key="list_of_users", task_ids="extract")
        tweets_list = ti.xcom_pull(key="list_of_tweets", task_ids="extract")

	user_df = {'user_id':[],'username':[],'name':[],'followers_count':[],
		'following_count':[],'tweet_count':[],'listed_count':[]}

	for user in users_list:
		user_df['user_id'].append(user['data']['id'])
		user_df['username'].append(user['data']['username'])
		user_df['name'].append(user['data']['name'])
		user_df['followers_count'].append(tweet['data']['public_metrics']['followers_count'])
		user_df['following_count'].append(tweet['data']['public_metrics']['following_count'])
		user_df['tweet_count'].append(tweet['data']['public_metrics']['tweet_count'])
		user_df['listed_count'].append(tweet['data']['public_metrics']['listed_count'])


	tweet_df = {'tweet_id':[],'text':[],'retweet_count':[],'reply_count':[],
		'like_count':[],'quote_count':[],'impression_count':[]}

	for tweet in tweets_list:
		tweet_df['tweet_id'].append(tweet['data']['id'])
		tweet_df['text'].append(tweet['data']['text'])
		tweet_df['retweet_count'].append(tweet['data']['public_metrics']['retweet_count'])
		tweet_df['reply_count'].append(tweet['data']['public_metrics']['reply_count'])
		tweet_df['like_count'].append(tweet['data']['public_metrics']['like_count'])
		tweet_df['quote_count'].append(tweet['data']['public_metrics']['quote_count'])
		tweet_df['impression_count'].append(tweet['data']['public_metrics']['impression_count'])
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
