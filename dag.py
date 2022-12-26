from datetime import datetime
from datetime import timedelta
from airflow.decorators import task, dag
import tweepy
import pandas as pd
import json
import s3fs
import re




@task
def get_data_from_twitter_api():

    api_key = "{/YOUR API KEY}"
    api_secret_key = "{/YOUR API SECRET KEY}"
    access_token = "{/YOUR ACCESS TOKEN}"
    access_secret_token = "{/YOUR ACCESS SECRET TOKEN}"

    auth = tweepy.OAuthHandler(api_key, api_secret_key)
    auth.set_access_token(access_token, access_secret_token)

    api = tweepy.API(auth)

    tweets = api.user_timeline(

        screen_name = '@GreatestQuotes',
        count = 200,
        include_rts = False,
        tweet_mode = 'extended'

    )

    ts = [t._json for t in tweets]

    return ts

@task
def transform_the_tweets(tweets):

    tweet_list = []

    for tweet in tweets:

        text = re.split(r'–| –| – |– |.  - |. -  |. - |. -|.-|.\n|-| -|- | - ', tweet['full_text'])

        quote = text[0]+'.'
        quote_by = text[1]

        data = {

            "username": tweet['user']['screen_name'],
            "quote" : quote,
            "quote by" : quote_by,
            "created time" : tweet['created_at'],
            "total like": tweet['favorite_count'],
            "total retweet" : tweet['retweet_count']

        }

        tweet_list.append(data)

    return tweet_list

@task
def load_into_s3(tweet_list):

    df = pd.DataFrame(tweet_list)
    df.to_csv("s3://arkan-twitter-to-s3-storage/quotes.csv")



default_args = {

    'owner' : 'arkan',
    'depends_on_past' : False,
    'start_date' : datetime(2023, 12, 26),
    'email' : ['airflow@example.com'],
    'email_on_failure' : False,
    'email_on_retry' : False,
    'retries' : 1,
    'retry_delay' : timedelta(minutes=1)

}

@dag('twitter_to_s3',default_args = default_args, 
description = 'extract tweet from twitter then transform it and load it into s3 bucket')
def may_dag():

    extract = get_data_from_twitter_api()
    transform = transform_the_tweets(extract)
    load_into_s3(transform)

may_dag()