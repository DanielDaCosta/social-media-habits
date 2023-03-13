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

# logging.basicConfig(filename='App_LikedTweets_ServerStatus.log', encoding='utf-8', level=logging.ERROR, force=True)

# Load Credentials
from dotenv import dotenv_values

# config = dotenv_values(".env")
BEARER_TOKEN_2 = 'AAAAAAAAAAAAAAAAAAAAAFcnhwEAAAAANY1uRtLTAKYYfaQE75bWfIMTNQ8%3DEJrT8tW7gSJTEWf7T0avi8APb8I21nsnv1n86EbjqQXhftxP34'
BEARER_TOKEN_3 = 'AAAAAAAAAAAAAAAAAAAAADLgHwEAAAAApdZg%2B4Z2%2BD0R%2B%2FkGysPFHrDtL3I%3DXD5bRneLXTjna5bdGMqBP7Qc5YcmuiMkZW4Grf9ZquovenzFaR'

BEARER_TOKEN = BEARER_TOKEN_2 if datetime.now().minute/2 < 20 else BEARER_TOKEN_3

def bearer_oauth(r):
    """
    Method required by bearer token authentication.
    """

    r.headers["Authorization"] = f"Bearer {BEARER_TOKEN}"
    r.headers["User-Agent"] = "v2RecentSearchPython"
    return r

def connect_to_endpoint(url, params):
    response = requests.get(url, auth=bearer_oauth, params=params)
    if response.status_code != 200:
        raise Exception(response.status_code, response.text)
    return response.json()

def main(url, params):
    json_response = connect_to_endpoint(url, params)
    # print(json.dumps(json_response, indent=4, sort_keys=True))
    return json_response

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

    current_time = datetime.now()

    date_str = f'{current_time.year}-{current_time.month:02d}-{current_time.day:02d}'
    time_str = f'{current_time.hour:02d}-{current_time.minute:02d}'

    sleep = 300 # in seconds
    N = 5 # Number of retries. Maxium retry time = N*sleep (s)

    # path = '/project/ll_774_951/SMHabits/avax_project_data/random_user_tweets'
    path = 'avax_data'
    dir_list = os.listdir(path)

    for batch_of_users, batch_csv in enumerate(dir_list):
        df = pd.read_csv(f"{path}/{batch_csv}")
        logging.info(f"Read {batch_csv}")

        # Optional params: start_time,end_time,since_id,until_id,max_results,next_token,
        # expansions,tweet.fields,media.fields,poll.fields,place.fields,user.fields
        query_params = {
            'user.fields': 'created_at,description,entities,id,location,name,pinned_tweet_id,profile_image_url,protected,public_metrics,url,username,verified,withheld',
            'tweet.fields': 'created_at,public_metrics,entities,lang,possibly_sensitive,reply_settings,source,in_reply_to_user_id,geo,referenced_tweets',
            'expansions': 'author_id',
        }

        hast_next_token = True
        users_id_list = set(df['userid'])
        # Create CSV file
        if not os.path.exists(f"data/liked-tweets-{batch_of_users}.csv"):
            with open(f"data/liked-tweets-{batch_of_users}.csv", "a+", encoding='utf-8') as f:
                f.write(", ".join(tweet_columns) + "\n")
        for userid in list(users_id_list): # There are 200 users id
            print(userid)
            hast_next_token = True
            while hast_next_token:
                print(hast_next_token)
                tweets_list_for_dataframe = []
                users_list_for_dataframe = []
                for attempt in range(N):
                    print(attempt)
                    try:
                        # Query Data from API
                        search_url = f"https://api.twitter.com/2/users/{userid}/liked_tweets"

                        # If user data has already been collected, skip
                        try:
                            save_csv = True
                            tweets_history = set(pd.read_csv('data/liked-tweets-0.csv', usecols=[' id'])[' id'].values)
                        except: # if file doesn't exist
                            tweets_history = {}

                        # query_params['query'] = f'(from:{userid})'
                        output_tweets = main(search_url, query_params)
                        logging.info(f'Twitter URL: {search_url}')
                        logging.info(f'Params: {query_params}')

                        # If no data found:
                        if not output_tweets.get('data'):
                            save_csv = False
                            logging.warning(f'No data found for userId {userid}')
                            # continue
                            hast_next_token = False
                            break
                        next_token = {}
                        next_token['pagination_token'] = output_tweets['meta'].get('next_token')
                        if next_token['pagination_token']:
                            query_params['pagination_token'] = next_token['pagination_token']
                            hast_next_token = True
                        else:
                            hast_next_token = False

                        for tweet in output_tweets['data']:
                            
                            # Check if tweet id has already been collected
                            if tweet['id'] in tweets_history:
                                logging.warning(f'Reapeated tweet_id for {userid}: stopping historical data collection')
                                print(f'Reapeated tweet_id for {userid}: stopping historical data collection')
                                hast_next_token = False # break from while loop
                                break

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
                            tweet = {**tweet, **entities_dict, **public_metrics_dict, **next_token, 'current_time': datetime.now()}
                            # tweet = sorted(tweet.items(), key=lambda kv: kv[1])
                            tweets_list_for_dataframe.append(tweet)

                    except Exception as err:
                        if err.args[0] == 429: # If ERROR = 429 (Too Many Requests, wait for retry) 
                            logging.info(f'Error 429. Sleep: {sleep}')
                            hast_next_token = False
                            time.sleep(sleep)
                            continue
                        else:
                            raise
                    break
                if save_csv:
                    tweets =  pd.DataFrame.from_records(tweets_list_for_dataframe, columns=tweet_columns)
                    tweets.to_csv(f'data/liked-tweets-{batch_of_users}.csv', mode='a', index=False, header=None)