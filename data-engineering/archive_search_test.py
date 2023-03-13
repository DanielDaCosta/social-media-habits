import logging

import pandas as pd
from collections import defaultdict
from datetime import datetime
from dateutil.relativedelta import relativedelta
import time
import os
import requests
import os
import json

logging.basicConfig(filename='AppServerStatus_script.log', encoding='utf-8', level=logging.DEBUG, force=True)

if __name__ == '__main__':
    json_tweets_columns = [
        'id', 'author_id', 'possibly_sensitive', 'edit_history_tweet_ids', 'lang',
        'source', 'reply_settings', 'text', 'created_at'
    ]

    json_users_columns = [
        'id', 'name', 'username', 'location', 'url', 'created_at', 'username',
        'profile_image_url', 'profile_image_url', 'verified', 'description',
        'protected'
    ]
    user_columns = [
        'profile_image_url', 'username', 'protected', 'name', 'id', 'description', 
        'created_at', 'verified', 'location', 'url_urls_list', 'description_hashtags_list', 'description_urls_list', 'description_mentions_list',
        'description_cashtags_list', 'public_metrics_followers_count', 'public_metrics_following_count', 'public_metrics_tweet_count', 
        'public_metrics_listed_count', 'current_time'
    ]

    tweet_columns = [
        'possibly_sensitive', 'text', 'source', 'id', 'created_at', 'lang', 'reply_settings', 'author_id', 'edit_history_tweet_ids', 
        'hashtags_list', 'urls_list', 'public_metrics_retweet_count', 'public_metrics_reply_count', 'public_metrics_like_count', 
        'public_metrics_quote_count', 'pagination_token', 'current_time'
    ]
    # Compute Start and End Time with 6 month interval

    END_TIME = '2022-10-01T00:00:00Z'
    END_TIME_DATETIME = datetime.strptime(END_TIME, '%Y-%m-%dT%H:%M:%SZ')
    START_TIME_DATETIME = END_TIME_DATETIME - relativedelta(months=6) # 6 Months Window
    START_TIME = START_TIME_DATETIME.strftime('%Y-%m-%dT%H:%M:%SZ')

    current_time = datetime.now()

    date_str = f'{current_time.year}-{current_time.month:02d}-{current_time.day:02d}'
    time_str = f'{current_time.hour:02d}-{current_time.minute:02d}'

    sleep = 300 # in seconds
    N = 5 # Number of retries. Maxium retry time = N*sleep (s)

    path = '/project/ll_774_951/SMHabits/avax_project_data/random_user_tweets'
    dir_list = os.listdir(path)
    batch_of_users = 99
    tweets =  pd.DataFrame.from_records([], columns=tweet_columns)
    tweets.to_csv(f'/home1/danielp3/data/tweets-{batch_of_users}.csv', mode='a', index=False)