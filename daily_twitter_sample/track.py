from utils import get_date_range, twitter_date_format_to_day, db
from count_or_search import count, use_x_as_id, form_count_query_params


def track_months(query: str, month: list):
    # todo make this check work better
    # check month is untracked before saving counts

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


def save_counts(query: str, months: list):
    # pull count of tweets for a month

    first, last = get_date_range(months)
    params = form_count_query_params(query, first, last)

    # set 10 percent of total count as target
    percent = 0.1
    tracker = []
    response = count(params)

    for day in response["data"]:
        tweet_count = day["tweet_count"]
        day_track = {
            "tweet_total": tweet_count,
            "tweet_target": round(tweet_count * percent),
            "tweet_current": 0,
            "_id": twitter_date_format_to_day(day["start"])
        }
        tracker.append(day_track)

    db["counts"].insert_many(tracker)
    # todo update to run for multiple months
    db["counts"].update_one({"track": "months"}, {"$addToSet": {"months": month}}, upsert=True)


def update_tracker(day: str, increment: int):
    # update tracker of number of tweets saved
    db["counts"].update_one({"_id": day}, {"$inc": {"tweet_current": increment}})


if __name__ == '__main__':
    track_months("bitcoin", ["Jan17", "Jun21"])
    # update_tracker("2018-01-01", 100)
    # print(json.dumps(num, indent=4, sort_keys=False))

