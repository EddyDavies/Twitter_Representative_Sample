import json

from track import save_times
from decorators import db, accept_duplicates


def store_tweets(response):

    processed_tweets = sort_includes(response)
    save_times(processed_tweets)

    save_users(response["includes"]["users"])

    tweets_added = len(processed_tweets)
    return tweets_added


@accept_duplicates
def save_users(users, sl=None):
    if sl is None:
        user_slice = users
    else:
        user_slice = users[sl]

    db["users"].insert_many(user_slice, ordered=False)


def sort_includes(response):

    processed_tweets = []
    for tweet in response["data"]:
        # check a tweet has been referenced
        if "referenced_tweets" in tweet:
            ref_tweet = tweet["referenced_tweets"][0]
            full_ref_tweet = response["includes"]["tweets"][0]

            if full_ref_tweet["_id"] == ref_tweet["id"]:  # todo check if this ever fails
                del response["includes"]["tweets"][0]

            # Compare tweet to retweet and select one with highest engagement
            if ref_tweet["type"] == "retweeted":
                processed_tweet = compare_retweet(tweet, full_ref_tweet)
            # Include public metrics of original tweet in this tweet
            elif ref_tweet["type"] == "quoted":
                processed_tweet = attach_quoted(tweet, full_ref_tweet)
            # Attach to tweet it the reply to
            elif ref_tweet["type"] == "replied_to":
                processed_tweet = attach_replied_to(tweet, full_ref_tweet)
        else:
            processed_tweet = tweet
        processed_tweets.append(processed_tweet)

    return processed_tweets


def attach_quoted(original_tweet, quoted_tweet):
    quoted_metrics = quoted_tweet["public_metrics"]
    original_tweet["quoted_metrics"] = quoted_metrics

    return original_tweet


def attach_replied_to(original_tweet, replied_to_tweet):
    del original_tweet["referenced_tweets"]
    replied_to_tweet["reply"] = original_tweet

    return replied_to_tweet


def compare_retweet(original_tweet, include_tweet):
    # compare the public metrics of two tweet and select most impactful

    if original_tweet["public_metrics"]["retweet_count"] == include_tweet["public_metrics"]["retweet_count"]:
        if original_tweet["public_metrics"]["like_count"] < include_tweet["public_metrics"]["like_count"]:
            selected_tweet = include_tweet
        else:
            selected_tweet = original_tweet
    else:
        if original_tweet["public_metrics"]["retweet_count"] < include_tweet["public_metrics"]["retweet_count"]:
            selected_tweet = include_tweet
        else:
            selected_tweet = original_tweet

    if selected_tweet == original_tweet:
        del selected_tweet["referenced_tweets"]

    return selected_tweet


def search_for_day(day):
    regex = f"^{day}*"
    return db["tweets"].find({"created_at": {"$regex": regex}})


if __name__ == '__main__':
    with open("../data/tweets2.json") as f:
        tweets = json.load(f)

    store_tweets(tweets)