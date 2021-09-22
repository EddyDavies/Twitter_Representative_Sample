import json
import time
from datetime import datetime, timedelta

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
                # Checks if anything has been provided to print before the time waited
                try:
                    begin_with = kwargs["begin"]
                    if not begin_with:
                        print(f"\r{begin_with}   Waited {seconds_to_sleep} seconds until {wait_until_time}", end="")
                    else:
                        raise KeyError
                except KeyError:
                    print(f"  Waited {seconds_to_sleep} seconds until {wait_until_time}", end="")
                time.sleep(seconds_to_sleep)
            return result
        return wrapped
    return wrapper