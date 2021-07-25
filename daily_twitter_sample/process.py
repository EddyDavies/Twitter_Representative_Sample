import json

from utils import db, twitter_date_format_to_day, string_to_month_year, ignore_duplicates, time_func


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


@time_func
@ignore_duplicates
def save_users(tweets, sl=None):
    if sl is None:
        users = tweets["includes"]["users"]
    else:
        users = tweets["includes"]["users"][sl]

    db["users"].insert_many(users, ordered=False)


if __name__ == '__main__':
    with open("../data/tweets.json") as f:
        tweets = json.load(f)
    with open("../data/tweets2.json") as f:
        tweets2 = json.load(f)

    # save_users(tweets, sl=slice(5))
    duration, _ = save_users(tweets)
    print(duration)

    # sort_through_includes(tweets)
    # db["tweets"].insert_many(tweets["data"])
    # db["tweets"].insert_many(tweets2["data"])

    # var = db["tweets"].find({}, {"text": 1, "public_metrics": 1, "_id": 0})
    # [print(x) for x in var]

    # search_by_day("2021-01-30")
    # save_times(tweets["data"])
