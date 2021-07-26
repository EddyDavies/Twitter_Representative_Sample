import json
import calendar
import os
import time
from datetime import datetime, timedelta, timezone
from typing import List

from pymongo import MongoClient
from pymongo.errors import BulkWriteError


def extract_env_vars():
    m = os.environ.get("TWITTER_DATE").split(" ")
    months_list = list(map(' '.join, zip(m[::2], m[1::2])))

    mongo = MongoClient(os.environ.get('MONGO_CLIENT'))
    q = os.environ.get('TWITTER_QUERY')
    dbname = os.environ.get('TWITTER_DBNAME', q.split(" ")[0] + "_extended")  # todo maybe change

    return months_list, q, mongo[dbname]


months, query, db = extract_env_vars()


def accept_duplicates(func):
    # define a decorator to ignore duplicate _id errors

    def wrap(*args, **kwargs):
        try:
            func(*args, **kwargs)
        except BulkWriteError as e:
            print("Duplicates present but ignoring error.")
            pass

    return wrap


def json_print(func):
    # Decorator that reports the execution time

    def wrap(*args, **kwargs):
        result = func(*args, **kwargs)
        print(json.dumps(result, indent=4, sort_keys=False))
        return result

    return wrap


def time_func(func):
    # Decorator that reports the execution time

    def wrap(*args, **kwargs):
        start = time.time()
        result = func(*args, **kwargs)
        end = time.time()

        duration = end - start
        return duration, result

    return wrap


def check_for_duplicates(dictionary_list, item):
    # check no duplicates in list of dictionaries

    items = []
    for dictionary in dictionary_list:
        items.append(dictionary[item])

    if len(items) == len(set(items)):
        return False
    else:
        return True


def twitter_date_format(date: str, end_of_day=False):
    # convert date in '%Y-%m-%d' format into twitter date format

    date_obj = datetime.strptime(date, '%Y-%m-%d')
    if end_of_day:
        date_obj = date_obj.replace(hour=23, minute=59, second=59)
    utc_date = date_obj.replace(tzinfo=timezone.utc)

    return str(utc_date).replace(" ", "T")


def twitter_date_format_to_day(date: str):
    # Get day in '%Y-%m-%d' format from exact twitter datetime

    date_obj = datetime.strptime(date.split('.', 1)[0], '%Y-%m-%dT%H:%M:%S')
    day = datetime.strftime(date_obj, '%Y-%m-%d')

    return day


def string_to_datetime(date: str):
    # get datetime object from string in '%Y-%m-%d' format
    return datetime.strptime(date, "%Y-%m-%d")


def string_to_month_year(date: str):
    return datetime.strptime(date, "%Y-%m-%d").strftime('%b %y')


# @json_print
def get_date_range(months: list):
    # returns first and last date for a specified month or range between 2 months

    first = datetime.strptime(months[0], "%b %y")

    if len(months) != 1:
        last_month = datetime.strptime(months[-1], "%b %y")
    else:
        last_month = first
    last = calendar.monthrange(int(last_month.strftime("%y")),
                               int(last_month.strftime("%m")))[1]
    last = datetime.strptime(f"{last} {months[-1]}", "%d %b %y")

    first = datetime.strftime(first, "%Y-%m-%d")
    last = datetime.strftime(last, "%Y-%m-%d")

    return first, last


# @json_print
def get_date_array(first: str, last: str):
    # get a list of dates from year and month name in '%Y-%m-%d' format

    current = datetime.strptime(first, "%Y-%m-%d")
    last = datetime.strptime(last, "%Y-%m-%d")

    date_array: List[str] = []
    end_of_range = True

    while end_of_range:
        current_string = datetime.strftime(current, "%Y-%m-%d")
        date_array.append(current_string)
        end_of_range = current != last
        current += timedelta(days=1)

    return date_array


if __name__ == "__main__":
    first, last = get_date_range(["Jan 17"])
    get_date_array(first, last)

