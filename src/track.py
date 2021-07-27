from datetime import datetime
from random import randrange

from utils import get_date_range, twitter_date_format_to_day, get_month_array, twitter_date_format_to_time
from decorators import db, accept_duplicates
from count_or_search import count, form_count_query_params


def check_counts(months_range: list):
    # checks the counts that are already stored on mongodb
    tracker = db["counts"].find_one({"_id": "track"}, {"months": 1, "_id": 0})
    if tracker is None:
        return [], months_range, []

    month_array = get_month_array(months_range)
    tracked_before, tracked_after = [], []
    first_untracked, last_untracked = None, None


    for month in month_array:
        last_month = month

        if month in tracker:
            if first_untracked is None:
                tracked_before.append(month)
            elif last_untracked is not None:
                tracked_after = []
        else:
            if first_untracked is None:
                first_untracked = month
            elif last_untracked is None or tracked_after is []:
                last_untracked = last_month
                tracked_after.append(month)
            else:
                tracked_after.append(month)

    return tracked_before, [first_untracked, last_untracked], tracked_after


@accept_duplicates
def create_counts(query: str, month_range: list, before: list, after: list, percent=0.1):
    # pull count of tweets for a month or range between two months

    months_array = get_month_array(month_range)

    first, last = get_date_range(month_range)
    params = form_count_query_params(query, first, last)
    counts = count_date_range(params)

    tracker = form_trackers(counts, percent)
    db["counts"].insert_many(tracker)

    full_months_array = before + months_array + after
    db["counts"].update_one({"_id": "months"}, {"$set": {"months":  full_months_array}}, upsert=True)


def count_date_range(params):
    # repeat response call until entire date range collected as no next token in provided
    counts = []
    response = count(params)
    while "next_token" in response["meta"]:
        params["next_token"] = response["meta"]["next_token"]
        response = count(params)
        new_counts = response["data"]
        new_counts.reverse()
        counts += new_counts
    return counts


def form_trackers(counts, percent):
    tracker = []
    for day in counts:
        tweet_count = day["tweet_count"]

        day_track = {
            "tweet_total": tweet_count,
            "tweet_target": round(tweet_count * percent),
            "tweet_current": 0,
            "_id": twitter_date_format_to_day(day["start"])
        }
        tracker.append(day_track)
    return tracker


def select_rand_time():
    # todo set so not near start of day
    hours, minutes, seconds = randrange(24), randrange(60), randrange(60)

    rough_time_string = f"{hours}:{minutes}:{seconds}"

    rand_time = datetime.strptime(rough_time_string, "%H:%M:%S").strftime("%H:%M:%S")
    return rand_time


def select_unused_time(day):
    used_times = db["counts"].find_one({"_id": day}, {"starts": 1, "ends": 1, "_id": 0})

    while True:
        selected_time = select_rand_time()
        if not used_times:
            return selected_time
        else:
            if time_new(selected_time, used_times):
                return selected_time


def time_new(selected_time, used_times):
    time_obj = datetime.strptime(selected_time, "%H:%M:%S")
    start_of_day = datetime.strptime("00:30:00", "%H:%M:%S")

    if time_obj < start_of_day:
        return False

    starts = used_times["starts"]
    ends = used_times["ends"]
    for x in range(len(starts)):
        if time_between(time_obj, starts[x], ends[x]):
            return False
    return True


def time_between(time_obj, start, end):
    start_obj = datetime.strptime(start, "%H:%M:%S")
    end_obj = datetime.strptime(end, "%H:%M:%S")

    if start_obj < time_obj < end_obj: # todo test to see if will overlap
        return True
    else:
        return False


if __name__ == '__main__':
    # track_months("bitcoin", ["Jan17", "Jun21"])
    # update_tracker("2018-01-01", 100)
    # print(json.dumps(num, indent=4, sort_keys=False))

    # months = get_month_array(["Jan 21", "Jun 21"])
    # save_counts("bitcoin", months)
    # db["counts"].update_one({"track": "months"}, {"$push": {"months": {"$each": months, "$position": -1}}}, upsert=True)

    total = 0
    cursor = db["counts"].find({"_id": {"$regex": '^\d*-\d*-\d*'}}, {"tweet_target": 1, "_id": 0})
    for day in cursor:
        total += day["tweet_target"]
        print(f"\rTotal={total}", end="")


