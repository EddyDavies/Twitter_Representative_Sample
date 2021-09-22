import os
from pymongo import MongoClient

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
