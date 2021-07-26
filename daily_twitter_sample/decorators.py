import json
import os
import time

from pymongo import MongoClient
from pymongo.errors import BulkWriteError


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


def extract_env_vars():
    m = os.environ.get("TWITTER_DATE").split(" ")
    months_list = list(map(' '.join, zip(m[::2], m[1::2])))

    mongo = MongoClient(os.environ.get('MONGO_CLIENT'))
    q = os.environ.get('TWITTER_QUERY')
    dbname = os.environ.get('TWITTER_DBNAME', q.split(" ")[0] + "_extended")
    # todo maybe change

    return months_list, q, mongo[dbname]


months, query, db = extract_env_vars()
