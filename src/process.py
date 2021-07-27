import json
import sys

from decorators import db, accept_duplicates
from utils import append_or_create_list, twitter_date_format_to_day, twitter_date_format_to_time


@accept_duplicates
def save_users(users, sl=None):
    if sl is None:
        user_slice = users
    else:
        user_slice = users[sl]

    db["users"].insert_many(user_slice, ordered=False)


@accept_duplicates
def save_tweets(tweets):
    db["tweets"].insert_many(tweets)


def save_extra_tweets(extra_day_tweets):
    # for each day save the tweets and update the tracker

    for day, tweets in extra_day_tweets.items():
        save_tweets(tweets)
        db["counts"].update_one({"_id": day}, {"$inc": {"tweet_current": len(tweets)}})


def store_tweets(response, day):

    processed_tweets, extra_day_tweets, first, last = sort_tweets(response, day)

    # save_tweets(processed_tweets, extra_day_tweets)
    save_tweets(processed_tweets)

    save_extra_tweets(extra_day_tweets)

    # Update the times covered in day tracker in counts collection
    db["counts"].update_one({"_id": day}, {"$addToSet": {"starts": first, "ends": last}})

    # track number of tweets added
    tweets_added = len(processed_tweets)
    db["counts"].update_one({"_id": day}, {"$inc": {"tweet_current": tweets_added}})

    save_users(response["includes"]["users"])

    return tweets_added


def sort_tweets(response, day):
    # sort through with references to attached tweets in each tweet
    processed_tweets = []
    referenced_tweets = {}

    for include_tweet in response["includes"]["tweets"]:
        referenced_tweets[include_tweet["_id"]] = {"include": include_tweet}

    for tweet in response["data"]:

        # check a tweet has been referenced
        if "referenced_tweets" in tweet:
            referenced_tweets = sort_referenced_tweet(tweet, referenced_tweets)
        else:
            processed_tweets.append(tweet)

    processed_ref_tweets = process_referenced(referenced_tweets)
    processed_ref_tweets, extra_day_tweets = extract_wrong_days(processed_ref_tweets, day)

    original_earliest_time = twitter_date_format_to_time(processed_tweets[-1]["created_at"])
    original_latest_time = twitter_date_format_to_time(processed_tweets[0]["created_at"])

    processed_tweets += processed_ref_tweets

    return processed_tweets, extra_day_tweets, original_earliest_time, original_latest_time


def sort_referenced_tweet(tweet, referenced_tweets):
    # sort all tweets with a reference into the type of reference and tweet they are a reference to

    ref_tweet_id = tweet["referenced_tweets"][0]["id"]
    ref_tweet_type = tweet["referenced_tweets"][0]["type"]

    # check if any in the includes list
    if ref_tweet_id in referenced_tweets:
        if ref_tweet_type == "retweeted":
            append_or_create_list(ref_tweet_type, referenced_tweets[ref_tweet_id], tweet)

        elif ref_tweet_type == "quoted":
            append_or_create_list(ref_tweet_type, referenced_tweets[ref_tweet_id], tweet)

        elif ref_tweet_type == "replied_to":
            append_or_create_list(ref_tweet_type, referenced_tweets[ref_tweet_id], tweet)

    else:
        append_or_create_list("sort_at_end", referenced_tweets, tweet)

    return referenced_tweets


def process_referenced(sorted_tweets):
    processed_tweets = []
    for _, tweet_references in sorted_tweets.items():

        # Include public metrics of original tweet in this tweet
        if "quoted" in tweet_references:
            new_tweets = attach_quoted(tweet_references["include"], tweet_references["quoted"])
            processed_tweets += new_tweets
        # Attach a reply to the original tweet
        if "replied_to" in tweet_references:
            new_tweet = attach_replied_to(tweet_references["include"], tweet_references["replied_to"])
            processed_tweets.append(new_tweet)
        # Compare tweet to retweet and select one with highest engagement
        if "retweeted" in tweet_references:
            new_tweets = compare_retweet(tweet_references["include"], tweet_references["retweeted"])
            processed_tweets += new_tweets

    return processed_tweets


# print(json.dumps(sorted_tweets, indent=4, sort_keys=False))
def attach_quoted(quoted_tweet, quoting_tweets):
    quoted_metrics = quoted_tweet["public_metrics"]
    for quoting_tweet in quoting_tweets:
        quoting_tweet["quoted_metrics"] = quoted_metrics

    return quoting_tweets


def attach_replied_to(parent_tweet, replied_tweets):
    for replied_tweet in replied_tweets:
        del replied_tweet["referenced_tweets"]
        append_or_create_list("reply", parent_tweet, replied_tweet)

    return parent_tweet


def compare_retweet(retweeted_tweet, retweets):
    # compare the public metrics of two tweet and select most impactful
    new_tweets = [retweeted_tweet]
    for retweet in retweets:
        retweet_like = retweet["public_metrics"]["like_count"]
        retweet_reply = retweet["public_metrics"]["reply_count"]
        retweet_quote = retweet["public_metrics"]["quote_count"]

        add_tweet = False
        if not (retweet_quote == 0 and retweet_reply == 0 and retweet_like == 0):
            if retweet_like >= retweeted_tweet["public_metrics"]["like_count"]:
                add_tweet = True
            if retweet_reply >= retweeted_tweet["public_metrics"]["reply_count"]:
                add_tweet = True
            if retweet_quote >= retweeted_tweet["public_metrics"]["quote_count"]:
                add_tweet = True

        if add_tweet:
            del retweet["referenced_tweets"]
            new_tweets.append(retweet)

    return new_tweets


def extract_wrong_days(processed_ref_tweets, day):
    current_day_tweets = []
    other_day_tweets = {}

    for tweet in processed_ref_tweets:
        current_day = twitter_date_format_to_day(tweet["created_at"])
        if current_day == day:
            current_day_tweets.append(tweet)
        else:
            append_or_create_list(current_day, other_day_tweets, tweet)

    return current_day_tweets, other_day_tweets


def fix_array_misalignment(day):
    buggy_tracker = db["counts"].find_one({"_id": {"$regex": day}}, {"_id": 0})
    if len(buggy_tracker["ends"]) > len(buggy_tracker["starts"]):
        print("   Last End " + buggy_tracker["ends"][-1])
        del buggy_tracker["ends"][-1]

    elif len(buggy_tracker["starts"]) > len(buggy_tracker["ends"]):
        print("   Last Start " + buggy_tracker["starts"][-1])
        del buggy_tracker["starts"][-1]

    db["counts"].replace_one({"_id": day}, buggy_tracker)


if __name__ == '__main__':

    day = "2017-04-13"
    if len(sys.argv) > 1:
        day = sys.argv[1]

    fix_array_misalignment(day)