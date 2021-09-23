import os
from pymongo import MongoClient

def extract_env_vars():
    m = os.environ.get("TWITTER_MONTHS")
    if m:
        m = m.split(" ")
        months = list(map(' '.join, zip(m[::2], m[1::2])))

    d = os.environ.get("TWITTER_DATES")
    if m:
        dates = d.split(" ")

    mongo_url = os.environ.get('MONGO_CLIENT')
    mongo = MongoClient(mongo_url)
    q = os.environ.get('TWITTER_QUERY')
    dbname = os.environ.get('TWITTER_DBNAME')
    # todo maybe change

    if m:
        print(f"Query: {q}, DBName: {dbname}, Date: {months[0]} to {months[1]}")
    elif d:
        print(f"Query: {q}, DBName: {dbname}, Date: {dates[0]} to {dates[1]}")

    print(mongo_url)

    return dates, months, q, mongo[dbname], mongo["shared"]


dates, months, query, db, db_share = extract_env_vars()
