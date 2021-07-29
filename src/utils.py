import calendar
import time
from datetime import datetime, timedelta, timezone
from typing import List

from decorators import db


def check_for_duplicates(dictionary_list, item):
    # check no duplicates in list of dictionaries

    items = []
    for dictionary in dictionary_list:
        items.append(dictionary[item])

    if len(items) == len(set(items)):
        return False
    else:
        return True


def twitter_date_format(date: str, time_string=None, end_of_day=False):
    # convert date in '%Y-%m-%d' format into twitter date format

    date_obj = datetime.strptime(date, '%Y-%m-%d')
    if time_string is not None:
        t = datetime.strptime(time_string, '%H:%M:%S')
        date_obj = date_obj.replace(
            hour=t.hour, minute=t.minute, second=t.second)

    elif end_of_day:
        date_obj = date_obj.replace(hour=23, minute=59, second=59)

    utc_date = date_obj.replace(tzinfo=timezone.utc)
    return str(utc_date).replace(" ", "T")


def twitter_date_format_to_day(date: str):
    # Get day in '%Y-%m-%d' format from exact twitter datetime

    date_obj = datetime.strptime(date.split('.', 1)[0], '%Y-%m-%dT%H:%M:%S')
    day = datetime.strftime(date_obj, '%Y-%m-%d')

    return day

def twitter_date_format_to_time(date: str):
    # Get day in '%Y-%m-%d' format from exact twitter datetime

    date_obj = datetime.strptime(date.split('.', 1)[0], '%Y-%m-%dT%H:%M:%S')
    time_string = datetime.strftime(date_obj, "%H:%M:%S")

    return time_string


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

    # set the last datetime to the last day in the month
    last = calendar.monthrange(int(last_month.strftime("%y")), int(last_month.strftime("%m")))[1]
    last = datetime.strptime(f"{last} {months[-1]}", "%d %b %y")

    first = datetime.strftime(first, "%Y-%m-%d")
    last = datetime.strftime(last, "%Y-%m-%d")

    return first, last


# @json_print
def get_date_array(date_range: tuple):
    # get a list of dates from year and month name in '%Y-%m-%d' format
    first, last = date_range

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


def get_month_array(month_range):
    # returns first and last month with year for a specified range between 2 months with years

    current_month = datetime.strptime(month_range[0], "%b %y")
    last_month = datetime.strptime(month_range[-1], "%b %y")
    month_array: List[str] = []
    end_of_range = True

    while end_of_range:
        current_string = datetime.strftime(current_month, "%b %y")
        month_array.append(current_string)

        next_month = int(datetime.strftime(current_month, "%-m"))
        next_year = int(datetime.strftime(current_month, "%Y"))

        if next_month == 12:
            next_month = 0
            next_year += 1
        next_month += 1

        end_of_range = current_month != last_month
        current_month = current_month.replace(month=next_month, year=next_year)

    return month_array


if __name__ == "__main__":
    print(get_date_array(get_date_range(["Jan 17"])))

    # months = get_month_array(["Jan 18", "Jun 21"])


def append_or_create_list(key: str, container: dict, content: dict):
    if key not in container:
        container[key] = [content]
    else:
        container[key].append(content)
    return container


def search_for_day(day):
    regex = f"^{day}*"
    return db["tweets"].find({"created_at": {"$regex": regex}})


def fix_array_misalignment(day):
    buggy_tracker = db["counts"].find_one({"_id": {"$regex": day}}, {"_id": 0})
    if len(buggy_tracker["ends"]) > len(buggy_tracker["starts"]):
        print("   Last End " + buggy_tracker["ends"][-1])
        del buggy_tracker["ends"][-1]

    elif len(buggy_tracker["starts"]) > len(buggy_tracker["ends"]):
        print("   Last Start " + buggy_tracker["starts"][-1])
        del buggy_tracker["starts"][-1]

    db["counts"].replace_one({"_id": day}, buggy_tracker)