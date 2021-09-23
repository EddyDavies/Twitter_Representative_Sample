import os
from pymongo import MongoClient

def extract_env_vars():
    dates_string = None

    m = os.environ.get("TWITTER_MONTHS")
    if m:
        m = m.split(" ")
        m = list(map(' '.join, zip(m[::2], m[1::2])))
        dates_string = f"{m[0]} to {m[1]}"

    d = os.environ.get("TWITTER_DATES")
    if d:
        d = d.split(" ")
        dates_string = f"{d[0]} to {d[1]}"

    mongo_url = os.environ.get('MONGO_CLIENT')
    mongo = MongoClient(mongo_url)
    q = os.environ.get('TWITTER_QUERY')
    dbname = os.environ.get('TWITTER_DBNAME')
    # todo maybe change

    print(f"Query: {q}, DBName: {dbname}, Date: {dates_string}")
    print(mongo_url)

    return d, m, q, mongo[dbname], mongo["shared"]


dates, months, query, db, db_share = extract_env_vars()
