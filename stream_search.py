import pandas as pd
from collections import defaultdict
from datetime import datetime
from dateutil.relativedelta import relativedelta
import time
import os
import logging
import json
import csv
import tweepy
import sys

logging.basicConfig(filename='App_Stream_ServerStatus.log', encoding='utf-8', level=logging.ERROR, force=True)

# Load Credentials
from dotenv import dotenv_values

config = dotenv_values(".env")
CONSUMER_KEY = config['CONSUMER_KEY']
CONSUMER_SECRET = config['CONSUMER_SECRET']
BEARER_TOKEN = config['BEARER_TOKEN']

def get_list_of_items(list_of_dict: list, key_name: str) -> list:
    if not list_of_dict: # If list_of_dict is None
        return []
    return [entity[key_name] for entity in list_of_dict]

def parse_entities(tweet_entities: dict, object_dict: dict, prefix: str = '') -> dict:
    entities_dict = defaultdict(list)

    # Retrieve Entities Objects
    for object_name, key_name in object_dict.items():
        column_name = f"{prefix}{object_name}_list"
        # print(object_name)
        entities_dict[column_name] = get_list_of_items(tweet_entities.get(object_name), key_name)
    return entities_dict
    # entities_dict['hashtags_list'] = get_list_of_items(tweet_entities['hashtags'], 'tag')
    # entities_dict['urls_list'] = get_list_of_items(tweet_entities['urls'], 'expanded_url')

def expand_dict_object(object_dict: dict, column_prefix: str,key_names_list: list = []) -> dict:
    """Return key,value pairs from dict. Returns selected keys in key_names_list or all if key_names_list is []

    Args:
        object_dict (dict)
        key_names_list (list)
    Returns:
        dict
    """
    result_dict = defaultdict(list)
    for key_name, value_name in object_dict.items():
        column_name = f"{column_prefix}_{key_name}"
        if not key_names_list:
            result_dict[column_name] = value_name
        elif key_name in key_names_list:
            result_dict[column_name] = value_name
    return result_dict


class StreamingClient(tweepy.StreamingClient):
    def on_data(self ,raw_data):
        json_tweets_columns = [
            'id', 'author_id', 'possibly_sensitive', 'edit_history_tweet_ids', 'lang',
            'source', 'reply_settings', 'text', 'created_at'
        ]

        json_users_columns = [
            'id', 'name', 'username', 'location', 'url', 'created_at', 'username',
            'profile_image_url', 'profile_image_url', 'verified', 'description',
            'protected'
        ]
        tweet_columns = [
            'possibly_sensitive', 'text', 'source', 'id', 'created_at', 'lang', 'reply_settings', 'author_id', 'edit_history_tweet_ids', 
            'hashtags_list', 'urls_list', 'public_metrics_retweet_count', 'public_metrics_reply_count', 'public_metrics_like_count', 
            'public_metrics_quote_count', 'pagination_token', 'current_time'
        ]

        user_columns = [
            'profile_image_url', 'username', 'protected', 'name', 'id', 'description', 
            'created_at', 'verified', 'location', 'url_urls_list', 'description_hashtags_list', 'description_urls_list', 'description_mentions_list',
            'description_cashtags_list', 'public_metrics_followers_count', 'public_metrics_following_count', 'public_metrics_tweet_count', 
            'public_metrics_listed_count', 'current_time'
        ]
        tweets_list_for_dataframe = []
        users_list_for_dataframe = []
        # print(raw_data)
        output_tweets = json.loads(raw_data)
        # If no data found:
        if output_tweets.get('data'):
            logging.warning(f'No data found for userId')
            # continue
            # hast_next_token = False
            # return
            tweet = output_tweets['data']
            # Get new columns
            if not tweet.get('entities'):
                entities_dict = parse_entities({}, {'hashtags': 'tag', 'urls': 'expanded_url'})
            else:
                entities_dict = parse_entities(tweet['entities'], {'hashtags': 'tag', 'urls': 'expanded_url'})

            if not tweet.get('public_metrics'):
                public_metrics_dict = expand_dict_object({}, 'public_metrics')
            else:
                public_metrics_dict = expand_dict_object(tweet['public_metrics'], 'public_metrics')

            # Filter out unwanted columns
            tweet = {key: tweet[key] for key in tweet.keys() if key in json_tweets_columns}

                            # Combine dicts
            tweet = {**tweet, **entities_dict, **public_metrics_dict, 'current_time': datetime.now()}
            with open(f"data/tweets-stream.csv", "a+", encoding='utf-8') as f:
                tweet_csv = [ tweet.get(col) for col in tweet_columns]
                write = csv.writer(f)
                write.writerows([tweet_csv])

            for user in output_tweets['includes']['users']:
                # Get new columns
                if (not user.get('entities')) or (not user.get('entities').get('url')):
                    url_dict = parse_entities({}, {'urls': 'expanded_url'}, prefix='url_')
                else:
                    url_dict = parse_entities(user['entities']['url'], {'urls': 'expanded_url'}, prefix='url_')

                if (not user.get('entities')) or (not user.get('entities').get('description')):
                    description_dict = parse_entities(
                        {},
                        {'hashtags': 'tag', 'urls': 'expanded_url', 'mentions': 'username', 'cashtags': 'tag'}, prefix='description_')
                else:
                    description_dict = parse_entities(
                        user['entities']['description'],
                        {'hashtags': 'tag', 'urls': 'expanded_url', 'mentions': 'username', 'cashtags': 'tag'}, prefix='description_')

                user_public_metrics_dict = expand_dict_object(user['public_metrics'], 'public_metrics')
                # Filter out unwanted columns
                user = {key: user[key] for key in user.keys() if key in json_users_columns}

                # Combine dicts
                user = {**user, **url_dict, **description_dict, **user_public_metrics_dict, 'current_time': datetime.now()}
                user_csv = [ user.get(col) for col in user_columns]
                users_list_for_dataframe.append(user_csv)

            if users_list_for_dataframe:
                with open(f"data/users-stream.csv", "a+", encoding='utf-8') as f:
                    write = csv.writer(f)
                    write.writerows(users_list_for_dataframe)

    def on_exception(self, exception):
        logging.warning(exception)
        print('On Exception')
    def on_errors(self, status_code):
        print("Encountered streaming error (", status_code, ")")
        # sys.exit()

if __name__ == '__main__':

    user_filters = "-is:retweet has:geo (from:NWSNHC OR from:NHC_Atlantic OR from:NWSHouston OR from:NWSSanAntonio OR from:USGS_TexasRain OR from:USGS_TexasFlood OR from:JeffLindner1)"
    streaming_client = StreamingClient(BEARER_TOKEN)
    streaming_client.add_rules(tweepy.StreamRule(user_filters))

    tweet_columns = [
        'possibly_sensitive', 'text', 'source', 'id', 'created_at', 'lang', 'reply_settings', 'author_id', 'edit_history_tweet_ids', 
        'hashtags_list', 'urls_list', 'public_metrics_retweet_count', 'public_metrics_reply_count', 'public_metrics_like_count', 
        'public_metrics_quote_count', 'pagination_token', 'current_time'
    ]

    user_columns = [
        'profile_image_url', 'username', 'protected', 'name', 'id', 'description', 
        'created_at', 'verified', 'location', 'url_urls_list', 'description_hashtags_list', 'description_urls_list', 'description_mentions_list',
        'description_cashtags_list', 'public_metrics_followers_count', 'public_metrics_following_count', 'public_metrics_tweet_count', 
        'public_metrics_listed_count', 'current_time'
    ]

    # Create CSV file
    if not os.path.exists(f"data/tweets-stream.csv"):
        with open("data/tweets-stream.csv", "a+", encoding='utf-8') as f:
            write = csv.writer(f)
            write.writerows([tweet_columns])

    if not os.path.exists(f"data/users-stream.csv"):
        with open("data/users-stream.csv", "a+", encoding='utf-8') as f:
            write = csv.writer(f)
            write.writerows([user_columns])

    streaming_client.sample(expansions='author_id', tweet_fields='created_at,public_metrics,entities,lang,possibly_sensitive,reply_settings,source,in_reply_to_user_id,geo', user_fields='created_at,description,entities,id,location,name,pinned_tweet_id,profile_image_url,protected,public_metrics,url,username,verified,withheld')