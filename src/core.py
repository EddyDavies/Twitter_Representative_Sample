import math
import subprocess

from count_or_search import search, form_search_query_params
from process import store_tweets
from track import create_counts, check_counts, select_unused_time
from utils import get_date_range, get_date_array
from decorators import months, query, db, minimum_execution_time


@minimum_execution_time(3, 1)
def collect(query, day, tweets_remaining):
    end = select_unused_time(day)

    max_results = 500
    if tweets_remaining < max_results:
        max_results = math.ceil(tweet_remaining/10)*10
        if max_results < 50:
            max_results += 20

    response = search(form_search_query_params(query, day, end, max_results))

    tweets_added = store_tweets(response, day)

    return tweets_added


if __name__ == '__main__':

    # check tracker exists for the whole specified region for this query
    tracker_matches, months_range = check_counts(months)
    if not tracker_matches:
        create_counts(query, months_range)

    dates = get_date_array(get_date_range(months))

    for day in dates:
        counts = db["counts"].find_one({"_id": day}, {"tweet_current": 1, "tweet_target": 1, "_id": 0})
        tweet_current, tweet_target = counts["tweet_current"], counts["tweet_target"]
        print(f"\n{tweet_current}/{tweet_target} Tweets Collected for {day}", end="")

        while tweet_current <= tweet_target:

            tweet_remaining = tweet_target - tweet_current
            tweet_added = collect(query, day, tweet_remaining)
            tweet_current += tweet_added

            print(f"\r{tweet_current}/{tweet_target} Tweets Collected for {day}", end="")
    

    subprocess.run(["shutdown", "-h", "now"])
