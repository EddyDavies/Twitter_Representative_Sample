from decorators import db, db_share, accept_duplicates
from utils import append_or_create_list, twitter_date_format_to_day, twitter_date_format_to_time


def store_tweets(response, day):

    new_tweets, combined_tweets, count, time = sort_tweets(response, day)

    save_tweets_to_mongo(new_tweets)
    replace_tweets_to_mongo(combined_tweets)

    # track number of tweets added
    db["counts"].update_one({"_id": day}, {"$inc": {"tweet_current": count}})

    # Update the times covered in day tracker in counts collection
    db["counts"].update_one({"_id": day}, {"$addToSet": {"times": time}})

    save_users_to_mongo(response["includes"]["users"])

    return count


def sort_tweets(response, day):
    # sort through with references to attached tweets in each tweet
    unprocessed_tweets, quotes_or_replies_ids, quotes_or_replies = [], [], []

    for tweet in response["data"]:
        # check a tweet has been referenced
        if "referenced_tweets" in tweet:
            if tweet["referenced_tweets"][0]["type"] != "retweeted":
                quotes_or_replies_ids.append(tweet["referenced_tweets"][0]["id"])
                quotes_or_replies.append(tweet)
            else:
                pass
        else:
            unprocessed_tweets.append(tweet)

    if "tweets" in response["includes"]:
        ref_tweets = response["includes"]["tweets"]
    else:
        ref_tweets = []
    combined_tweets, unedited_tweets = sort_referenced_tweet(
        quotes_or_replies, quotes_or_replies_ids, ref_tweets)

    new_tweets = unprocessed_tweets + unedited_tweets
    all_tweets = unprocessed_tweets + unedited_tweets + combined_tweets
    count = count_wrong_days(all_tweets, day)

    earliest_time = twitter_date_format_to_time(unprocessed_tweets[-1]["created_at"])
    latest_time = twitter_date_format_to_time(unprocessed_tweets[0]["created_at"])

    return new_tweets, combined_tweets, count, [earliest_time, latest_time]


def sort_referenced_tweet(tweets, ids, ref_tweets):
    combined_tweets, used_replies_ids, unedited_tweets = [], [], []

    for tweet in tweets:
        new_tweet = tweet
        tweet_id = tweet["referenced_tweets"][0]["id"]
        tweet_type = tweet["referenced_tweets"][0]["type"]
        start = 0

        if tweet_id in ids:
            while True:
                try:
                    if tweet_type == "quoted":
                        position = ids.index(tweet_id)
                        ref_tweet = ref_tweets[position]
                        new_tweet = attach_quoted(new_tweet, ref_tweet)
                        break
                    elif tweet_type == "replied_to":
                        position = ids.index(tweet_id, start)
                        ref_tweet = ref_tweets[position]
                        new_tweet = attach_replied_to(new_tweet, ref_tweet)
                        used_replies_ids.append(tweet_id)
                        start = position + 1
                except Exception as e:
                    break
            combined_tweets += [new_tweet]
        else:
            unedited_tweets += [new_tweet]
    return combined_tweets, unedited_tweets


def attach_quoted(quoting_tweet, quoted_tweet):
    quoted_metrics = quoted_tweet["public_metrics"]
    del quoting_tweet["referenced_tweets"]
    quoting_tweet = append_or_create_list("quoted_metrics", quoting_tweet, quoted_metrics)

    return quoting_tweet


def attach_replied_to(replied_tweet, parent_tweet):
    del parent_tweet["referenced_tweets"]
    del parent_tweet["in_reply_to_user_id"]
    append_or_create_list("reply", parent_tweet, replied_tweet)

    return parent_tweet


def count_wrong_days(all_tweets, day):
    day_count_tweets = 0

    for tweet in all_tweets:
        current_day = twitter_date_format_to_day(tweet["created_at"])
        if current_day == day:
            day_count_tweets += 1

    return day_count_tweets


@accept_duplicates
def save_tweets_to_mongo(tweets):
    db["tweets"].insert_many(tweets)


def replace_tweets_to_mongo(tweets):
    for tweet in tweets:
        tweet_id = tweet["_id"]
        del tweet["_id"]
        db["tweets"].replace_one({"_id": tweet_id}, tweet)


@accept_duplicates
def save_users_to_mongo(users, sl=None):
    if sl is None:
        user_slice = users
    else:
        user_slice = users[sl]

    db_share["users"].insert_many(user_slice, ordered=False)


if __name__ == '__main__':

    # day = "2017-04-13"
    # if len(sys.argv) > 1:
    #     day = sys.argv[1]
    #
    # fix_array_misalignment(day)

    obj_ids = ["848107827100958720", "848107825939136512", "848107825058373633", "848107822864781312"]
    cur = db.tweets.find({"_id": {"$in": obj_ids}})
    for x in cur:
        print(x)
