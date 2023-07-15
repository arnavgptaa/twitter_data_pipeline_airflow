import tweepy
import pandas as pd
import json
from datetime import datetime
import s3fs

def run_twitter_etl():

    access_key = "vTCzNr0RT6H5pxx9RTR23KnlK"
    access_secret = "UYQLjRfhkVDDaNvPpvXfliBPnoHA4INxTiSDi8vN9QMUeSS3SH"
    consumer_key = "1289849652213235712-xKeeva0PrxCxWQJLVBRVV1Q8Ls99Oc"
    consumer_secret = "oIngeyjXGv9us6On328sPSEOL2T4AuW6Smm9Qrnh91Uzs"

    # Twitter Authentication
    auth = tweepy.OAuthHandler(access_key, access_secret)
    auth.set_access_token(consumer_key, consumer_secret)

    # creating an API object
    api = tweepy.API(auth)

    tweets = api.user_timeline(screen_name = '@elonmusk',
                            count = 200,             # maximum allowed count
                            include_rts = False,     # including retweets kept as false
                            tweet_mode = 'extended'  # to show the entire tweet
                            )
    list = []
    for tweet in tweets:
        text = tweet._json["full_text"]

        refined_tweet = {"user": tweet.user.screen_name,
                            'text' : text,
                            'favorite_count' : tweet.favorite_count,
                            'retweet_count' : tweet.retweet_count,
                            'created_at' : tweet.created_at}
            
        list.append(refined_tweet)

    df = pd.DataFrame(list)
    df.to_csv('refined_tweets.csv')