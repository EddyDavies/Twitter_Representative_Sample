import requests
import os

from query_helper import twitter_date_format

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
    if response.status_code != 200:
        raise Exception(response.status_code, response.text)
    assert response.json() is not None
    return response.json()


def count(query_params):
    return connect_to_endpoint(count_url, query_params)


def search(query_params):
    return convert_id_across_response(connect_to_endpoint(search_url, query_params))


def convert_id_across_response(data):
    data["data"] = use_x_as_id(data["data"])
    if data["includes"] is not None:
        if data["includes"]["tweets"] is not None:
            data["includes"]["tweets"] = use_x_as_id(data["includes"]["tweets"])
        if data["includes"]["users"]:
            data["includes"]["users"] = use_x_as_id(data["includes"]["users"])
    return data


def use_x_as_id(data, x="id"):
    for item in data:
        item["_id"] = item[x]
        del item[x]
    return data


if __name__ == "__main__":
    query = "bitcoin"

    # todo do I need in reply_to_user_id ?
    search_query_params = {
        'query': query + " lang:en",
        'max_results': 10,
        'start_time': twitter_date_format('2021-01-05'),
        'end_time': twitter_date_format('2021-01-30'),
        'tweet.fields': 'text,created_at,id,public_metrics',
        'user.fields': 'public_metrics',
        'expansions': 'referenced_tweets.id,author_id,referenced_tweets.id.author_id,in_reply_to_user_id'
    }
    # json_response = search(search_query_params)
    # print(json.dumps(json_response, indent=4, sort_keys=False))

    count_query_params = {
        'query': query + " lang:en",
        'granularity': 'day',
        'start_time': '2021-01-01T00:00:00Z',
        'end_time': '2021-02-01T00:00:00Z'
    }
    # json_response = count(count_query_params)
    # print(json.dumps(json_response, indent=4, sort_keys=False))