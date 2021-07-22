
import requests
import os
import json

from twitter.prep_query import twitter_date_format

search_url = "https://api.twitter.com/2/tweets/search/all"
count_url = "https://api.twitter.com/2/tweets/counts/all"


def bearer_oauth(r):
    """
    Method required by bearer token authentication.
    """
    # export 'BEARER_TOKEN'='<your_bearer_token>'
    bearer_token = os.environ.get("BEARER_TOKEN")
    r.headers["Authorization"] = f"Bearer {bearer_token}"
    return r


def connect_to_endpoint(url, params):
    response = requests.request("GET", url, auth=bearer_oauth, params=params)
    print(response.status_code)
    if response.status_code != 200:
        raise Exception(response.status_code, response.text)
    assert response.json() is not None
    return response.json()


def count(query_params):
    return connect_to_endpoint(count_url, query_params)


def search(query_params):
    return connect_to_endpoint(search_url, query_params)


if __name__ == "__main__":
    query = "bitcoin"

    search_query_params = {
        'query': query + " lang:en",
        'max_results': 10,
        'start_time': twitter_date_format('2021-01-05'),
        'end_time': twitter_date_format('2021-01-31'),
        'tweet.fields': 'text,created_at,id,public_metrics,user_id',
        'expansions': 'referenced_tweets.id'
    }
    # Optional params: start_time,end_time,since_id,until_id,max_results,next_token,
    # expansions,tweet.fields,media.fields,poll.fields,place.fields,user.fields
    json_response = search(search_query_params)
    print(json.dumps(json_response, indent=4, sort_keys=False))

    # Optional params: start_time,end_time,since_id,until_id,next_token,granularity
    count_query_params = {
        'query': query + " lang:en",
        'granularity': 'day',
        'start_time': '2021-01-01T00:00:00Z',
        'end_time': '2021-02-01T00:00:00Z'
    }
    json_response = count(count_query_params)
    print(json.dumps(json_response, indent=4, sort_keys=False))

