import os
import json
import calendar
from datetime import datetime, timedelta, timezone


# create search query params in twitter required format
def form_count_query_params(query, start, end):

    count_query_params = {
        'query': query + " lang:en",
        'granularity': 'day',
        'start_time': twitter_date_format(start),
        'end_time': twitter_date_format(end)
    }
    return count_query_params


# create count query params in twitter required format
def form_search_query_params(query, start, end, max_results=10):
    search_query_params = {
        'query': query + " lang:en",
        'max_results': max_results,
        'start_time': twitter_date_format(start),
        'end_time': twitter_date_format(end),
    }
    return search_query_params


# convert date in '%Y-%m-%d' format into twitter date format
def twitter_date_format(date):
    date_obj = datetime.strptime(date, '%Y-%m-%d')
    twitter_date = str(date_obj.replace(
        tzinfo=timezone.utc)).replace(" ", "T")
    
    return twitter_date


# Get day in '%Y-%m-%d' format from exact twitter datetime
def twitter_date_format_to_day(date):
    date_obj = datetime.strptime(date.split('.', 1)[0], '%Y-%m-%dT%H:%M:%S')
    day = datetime.strftime(date_obj, '%Y-%m-%d')

    return day


# extract first and last day of month from a year and month name
def get_date_range(date_string):
    first = datetime.strptime(date_string, "%b%y")

    last = calendar.monthrange(int(first.strftime("%y")), int(first.strftime("%m")))[1]
    last = datetime.strptime(f"{last}{date_string}", "%d%b%y")

    first = datetime.strftime(first, "%Y-%m-%d")
    last = datetime.strftime(last, "%Y-%m-%d")

    return first, last


# get a list of dates from year and month name in '%Y-%m-%d' format
def get_date_array(date_string):
    first_string, last_string = get_date_range(date_string)

    last = datetime.strptime(last_string, "%Y-%m-%d")
    current = datetime.strptime(first_string, "%Y-%m-%d")

    date_array = []
    end_of_range = True
    while end_of_range:
        current_string = datetime.strftime(current, "%Y-%m-%d")
        date_array.append(current_string)
        end_of_range = current != last
        current += timedelta(days=1)
    return date_array


# get datetime object from string in '%Y-%m-%d' format
def string_to_datetime(date_string):
    return datetime.strptime(date_string, "%Y-%m-%d")


if __name__ == "__main__":
    print(json.dumps(get_date_array("Jan17"), indent=4, sort_keys=False))