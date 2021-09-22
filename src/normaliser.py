import sys
import re
import time

import pandas as pd
from nltk.tokenize import TweetTokenizer
from emoji import demojize

tokenizer = TweetTokenizer()


def normalizeToken(token):
    lowercased_token = token.lower()
    if token.startswith("@"):
        return "@USER"
    elif lowercased_token.startswith("http") or lowercased_token.startswith("www"):
        return "HTTPURL"
    elif len(token) == 1:
        return demojize(token)
    else:
        if token == "’":
            return "'"
        elif token == "…":
            return "..."
        else:
            return token


def normalizeTweet(tweet):
    tokens = tokenizer.tokenize(tweet.replace("’", "'").replace("…", "..."))
    normTweet = " ".join([normalizeToken(token) for token in tokens])

    normTweet = normTweet.replace("cannot ", "can not ").replace("n't ", " n't ").replace("n 't ", " n't ").replace(
        "ca n't", "can't").replace("ai n't", "ain't")
    normTweet = normTweet.replace("'m ", " 'm ").replace("'re ", " 're ").replace("'s ", " 's ").replace("'ll ",
                                                                                                         " 'll ").replace(
        "'d ", " 'd ").replace("'ve ", " 've ")
    normTweet = normTweet.replace(" p . m .", "  p.m.").replace(" p . m ", " p.m ").replace(" a . m .",
                                                                                            " a.m.").replace(" a . m ",
                                                                                                             " a.m ")

    normTweet = re.sub(r",([0-9]{2,4}) , ([0-9]{2,4})", r",\1,\2", normTweet)
    normTweet = re.sub(r"([0-9]{1,3}) / ([0-9]{2,4})", r"\1/\2", normTweet)
    normTweet = re.sub(r"([0-9]{1,3})- ([0-9]{2,4})", r"\1-\2", normTweet)

    return " ".join(normTweet.split())


def normalise_csv(csv_path, col_num=-1, start=0):
    DATASET_ENCODING = "ISO-8859-1"
    print("loading...")
    df = pd.read_csv(csv_path, encoding=DATASET_ENCODING)

    col = "tweets"
    id = 1
    estimate_calculated = 0
    start = int(start)

    if col_num != -1:
        header_names = list(range(df.shape[1]))
        header_names[int(col_num)-1] = col
        df.columns = header_names
    total = df.shape[0]

    split_path = csv_path.split(".")
    edit_path = f"{split_path[0]}_from_{start}.csv"

    final_df = df.loc[start:].copy()
    df = df.to_dict()
    begin = time.time()


    for index, tweet in df["tweets"].items():
        if index > start:
            new_tweet = str(normalizeTweet(tweet))
            final_df.loc[index, "tweets"] = new_tweet
            if index % 5000 == 0:
                final_df.loc[:index].to_csv(edit_path, index=False, header=False)
                estimate = calculate_estimate(start, index, total, begin)
                print(f"\r{index}/{total} Line Saved, ETC: {estimate}")
            elif index % 200 == 0:
                if estimate_calculated < 5:
                    estimate = calculate_estimate(start, index, total, begin)
                    estimate_calculated += 1
                print(f"\r{index}/{total} Line Added, ETC: {estimate}", end="")

    final_df.to_csv(edit_path, index=False, header=False)
    print(f"\r{index + 1}/{total} Line Saved")


def calculate_estimate(start, index, total, script_begin):
    now = time.time()
    duration = now - script_begin

    run_length = index - start
    run_total = total - start

    estimate_length = (duration * (run_total / run_length))
    estimate_end = estimate_length + script_begin

    estimate = time.ctime(estimate_end)

    return estimate


if __name__ == "__main__":
    # df = pd.read_csv("/Users/edwarddavies/Git/Trade_with_Twitter/data/test.csv")

    if len(sys.argv) == 2:
        normalise_csv(sys.argv[1])
    if len(sys.argv) > 2:
        normalise_csv(sys.argv[1], *sys.argv[2:])
    else:
        print(normalizeTweet(
            "SC has first two presumptive cases of coronavirus, DHEC confirms https://postandcourier.com/health/covid19/sc-has-first-two-presumptive-cases-of-coronavirus-dhec-confirms/article_bddfe4ae-5fd3-11ea-9ce4-5f495366cee6.html?utm_medium=social&utm_source=twitter&utm_campaign=user-share… via @postandcourier"))