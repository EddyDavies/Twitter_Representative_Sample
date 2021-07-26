from datetime import time

from count_or_search import search, form_search_query_params
from process import store_tweets
from track import create_counts, update_tracker, check_counts, select_unused_time
from utils import get_date_range, get_date_array
from decorators import months, query, db, minimum_execution_time


@minimum_execution_time(3, 1)
def collect(query, day):
    end = select_unused_time(day)
    response = search(form_search_query_params(query, day, end, 500))

    tweets_added = store_tweets(response)
    update_tracker(day, tweets_added)

    return tweets_added


if __name__ == '__main__':

    # check tracker exists for the whole specified region for this query
    tracked_before, untracked_months, tracked_after = check_counts(months)
    create_counts(query, untracked_months, tracked_before, tracked_after)

    dates = get_date_array(get_date_range(months))

    for day in dates:
        counts = db["counts"].find_one({"_id": day}, {"tweet_current": 1, "tweet_target": 1, "_id": 0})
        tweet_current, tweet_target = counts["tweet_current"], counts["tweet_target"]
        print(f"\n{tweet_current}/{tweet_target} Tweets Collected for {day}", end="")

        while tweet_current <= tweet_target:
            print(f"\r{tweet_current}/{tweet_target} Tweets Collected for {day}", end="")

            tweet_added = collect(query, day)
            tweet_current += tweet_added
