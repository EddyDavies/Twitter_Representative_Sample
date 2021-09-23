import os
from pymongo import MongoClient

def extract_env_vars():
    m = os.environ.get("TWITTER_MONTHS")
    if m:
        m = m.split(" ")
        m = list(map(' '.join, zip(m[::2], m[1::2])))

    d = os.environ.get("TWITTER_DATES")
    if m:
        d = d.split(" ")

    mongo_url = os.environ.get('MONGO_CLIENT')
    mongo = MongoClient(mongo_url)
    q = os.environ.get('TWITTER_QUERY')
    dbname = os.environ.get('TWITTER_DBNAME')
    # todo maybe change

    if m:
        print(f"Query: {q}, DBName: {dbname}, Date: {m[0]} to {m[1]}")
    elif d:
        print(f"Query: {q}, DBName: {dbname}, Date: {d[0]} to {d[1]}")

    print(mongo_url)

    return d, m, q, mongo[dbname], mongo["shared"]


dates, months, query, db, db_share = extract_env_vars()
