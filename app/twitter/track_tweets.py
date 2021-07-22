import os

from pymongo import MongoClient

from twitter.count_or_search import count
from twitter.query_helper import get_date_range, form_count_query_params, twitter_date_format_to_day

# todo Confirm this used across program
mongo = MongoClient(os.environ.get('MONGO_CLIENT'))
db = mongo[os.environ.get('TWITTER_DBNAME', 'bitcoin')]


# update tracker of number of tweets saved
def update_tracker(day, increment):
    db["counts"].update_one({"date": day}, {"$inc": {"tweet_current": increment}})


# check month is untracked before saving counts
def track_months(query, month):
    tracked_months = db["counts"].find_one({"track": "months", "months": month}, {"months": 1, '_id': 0})
    run = True
    if tracked_months is not None:
        if month not in tracked_months["months"]:
            run = True
        else:
            run = False
    if run:
        save_counts(query, month)
        print(f"{month} has been added to MongoDB counts")
    else:
        print(f"{month} was already saved in MongoDB counts")


# pull count of tweets for a month
def save_counts(query, month):
    first, last = get_date_range(month)
    params = form_count_query_params(query, first, last)

    # set 10 percent of total count as target
    percent = 0.1
    tracker = []
    response = count(params)

    for day in response["data"]:
        tweet_count = day["tweet_count"]
        day_track = {
            "tweet_total": tweet_count,
            "tweet_target": round(tweet_count*percent),
            "tweet_current": 0,
            "date": twitter_date_format_to_day(day["start"])
        }
        tracker.append(day_track)

    db["counts"].insert_many(tracker)
    db["counts"].update_one({"track": "months"},  {"$addToSet": {"months": month}}, upsert=True)




if __name__ == '__main__':
    # track_months("bitcoin", "Jan18")
    # update_tracker("2018-01-01", 100)
    # print(json.dumps(num, indent=4, sort_keys=False))
