import os
import pandas as pd

from normaliser import normalizeTweet
from mongo import db, months, dates
from utils import get_date_array, get_date_range, string_to_month_year


def find_days_docs(day, db):
    regex = f"^{day}*"
    return db["tweets"].find({"created_at": {"$regex": regex}})


if __name__ == '__main__':
    # get a list of all the days in the
    if months:
        date_range = get_date_array(get_date_range(months))
    elif dates:
        dates_tup = (dates[0], dates[1])
        date_range = get_date_array(dates_tup)


    for day in date_range:
        df = pd.DataFrame()


        month_year = string_to_month_year(day)
        outdir = f"../data/{month_year}"
        if not os.path.exists(outdir):
            os.makedirs(outdir)
        outname = f"MTurk_{day}.csv"
        fullname = os.path.join(outdir, outname)
        if os.path.exists(fullname):
            print(f"{day} already saved.")
        else:
            # Collect all the tweets for the given day
            data = find_days_docs(day, db)

            for tweet in data:
                tweet_text = normalizeTweet(tweet["text"])
                # tweet_text = tweet["text"].replace('\n', ' ').replace('\r', '')
                # tweet_text = emoji.demojize(tweet_text)
                new_row = {'id': tweet["_id"], 'tweet': tweet_text, 'coin': "bitcoin"}
                df = df.append(new_row, ignore_index=True)

            df.to_csv(fullname)
            print(f"Save {day} as csv")
    # df = pd.read_csv("../data/MTurk_2021-01-01.csv")
    # print(df.head())
