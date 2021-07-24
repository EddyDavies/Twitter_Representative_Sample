import json
import os
from pymongo.errors import BulkWriteError
from pymongo import MongoClient

# todo Confirm this used across program
from twitter.count_or_search import use_x_as_id
from twitter.query_helper import twitter_date_format, twitter_date_format_to_day, string_to_month_year

mongo = MongoClient(os.environ.get('MONGO_CLIENT'))
db = mongo[os.environ.get('TWITTER_DBNAME', 'bitcoin')]


# todo replace with function that uses mongo search
# compares the tweet to linked include and select most interacted


# collect user followings
# select random time for next tweet
# check random time does not overlap
# collect tweets at time


def sort_through_includes(tweets):
    processed_tweets = []
    # print(json.dumps(tweets, indent=4, sort_keys=False))
    for tweet in tweets["data"]:
        # check a tweet has been referenced
        if tweet["referenced_tweets"] is not None:
            ref_tweet = tweet["referenced_tweets"][0]
            if ref_tweet["type"] == "retweeted":
                # Compare tweet to retweet and select one with highest engagement
                include_tweet = tweets["includes"]["tweets"].pop(0)
                if include_tweet["id"] == ref_tweet["id"]:  # todo check if this ever fails
                    selected_tweet = compare_tweets(tweet, include_tweet)
            elif ref_tweet["replied_to"]:
                pass
        #     todo check for quote tweet and
        else:
            selected_tweet = tweet

        # author id
        processed_tweets.append(selected_tweet)


def compare_tweets(original_tweet, include_tweet):
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


def collect_follower_counts():
    


# check no duplicates in list of dictionaries
def check_for_duplicates(dictionary_list, item):
    items = []
    for dictionary in dictionary_list:
        items.append(dictionary[item])

    if len(items) == len(set(items)):
        return False
    else:
        return True


def search_by_day(day):
    regex = f"^{day}*"
    return db["tweets"].find({"created_at": {"$regex": regex}})


def save_times(tweets):
    first = tweets[0]["created_at"]
    last = tweets[-1]["created_at"]

    date = twitter_date_format_to_day(first)
    try:
        db["counts"].update_one({"_id": date}, {"$addToSet": {"starts": first, "ends": last}})
        print(f"First {first} and Last {last} added to {date}")
    except:
        print(f"Counts for {string_to_month_year(date)} have not yet been generated")


if __name__ == '__main__':
    with open("/Users/edwarddavies/Documents/Git/Twitter_Data_Collection/tweets.json") as f:
        tweets = json.load(f)
    with open("/Users/edwarddavies/Documents/Git/Twitter_Data_Collection/tweets2.json") as f:
        tweets2 = json.load(f)

    # sort_through_includes(tweets)
    # db["tweets"].insert_many(tweets["data"])
    # db["tweets"].insert_many(tweets2["data"])

    # try:
    #     db["users"].insert_many(tweets["includes"]["users"], ordered=False)
    # except BulkWriteError as e:
    #     pass
    # print(len(tweets["includes"]["users"]))

    # var = db["tweets"].find({}, {"text": 1, "public_metrics": 1, "_id": 0})
    # [print(x) for x in var]

    # search_by_day("2021-01-30")
    save_times(tweets["data"])
