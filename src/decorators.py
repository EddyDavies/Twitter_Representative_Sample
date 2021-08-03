import json
import os
import time
from functools import wraps
from datetime import datetime, timedelta

from pymongo import MongoClient
from pymongo.errors import BulkWriteError


def accept_duplicates(func):
    # define a decorator to ignore duplicate _id errors

    def wrap(*args, **kwargs):
        try:
            func(*args, **kwargs)
        except BulkWriteError as e:
            # print("Duplicates present but ignoring error.")
            pass

    return wrap


def json_print(func):
    # Decorator that reports the execution time

    def wrap(*args, **kwargs):
        result = func(*args, **kwargs)
        print(json.dumps(result, indent=4, sort_keys=False))
        return result

    return wrap


def minimum_execution_time(seconds=3, microseconds=1):

    def wrapper(func):
        def wrapped(*args, **kwargs):
            wait_until_time = datetime.utcnow() + timedelta(seconds=seconds, microseconds=microseconds)
            result = func(*args, **kwargs)
            if datetime.utcnow() < wait_until_time:
                seconds_to_sleep = (wait_until_time - datetime.utcnow()).total_seconds()
                print(f"  Waited {seconds_to_sleep} seconds until {wait_until_time}", end="")
                time.sleep(seconds_to_sleep)
            return result
        return wrapped
    return wrapper


def extract_env_vars():
    m = os.environ.get("TWITTER_DATE").split(" ")
    months_list = list(map(' '.join, zip(m[::2], m[1::2])))

    mongo_url = os.environ.get('MONGO_CLIENT')
    mongo = MongoClient(mongo_url)
    q = os.environ.get('TWITTER_QUERY')
    dbname = os.environ.get('TWITTER_DBNAME')
    # todo maybe change

    print(f"Query: {q}, DBName: {dbname}, Date: {months_list[0]} to {months_list[1]}")
    print(mongo_url)

    return months_list, q, mongo[dbname], mongo["shared"]


months, query, db, db_share = extract_env_vars()
