import tweepy
import pandas as pd
import json
from datetime import datetime
import s3fs
import time
import os
from dotenv import load_dotenv

def twitter_ETL():
    load_dotenv()
    bearer_token = os.getenv("BEARER_TOKEN")

    # twitter authentication
    auth = tweepy.Client(bearer_token)

    # get user ID
    def get_user_id(username):
        while True:
            try:
                user = auth.get_user(username=username)
                return user.data.id
            except tweepy.TooManyRequests:
                print("Rate limit hit. Sleep for 15 minutes...")

    # get tweets
    def get_recent_tweets(user_id, max_results=1):
        while True:
            try:
                tweets = auth.get_users_tweets(
                    id=user_id,
                    max_results=max_results,  
                    tweet_fields=["created_at", "public_metrics", "text"]
                )
                return tweets.data
            
            except tweepy.TooManyRequests:
                print("Rate limit hit. Sleep for 15 minutes...")

    # format data frame
    def tweets_to_df(tweets):
        if not tweets:
            return pd.DataFrame()

        data = [{
            "created_at": tweet.created_at,
            "text": tweet.text,
            "retweets": tweet.public_metrics["retweet_count"],
            "likes": tweet.public_metrics["like_count"],
            "replies": tweet.public_metrics["reply_count"],
            "quotes": tweet.public_metrics["quote_count"]
        } for tweet in tweets]

        return pd.DataFrame(data)

    # usage
    username = "elonmusk"
    user_id = get_user_id(username)
    tweets = get_recent_tweets(user_id, max_results=1)

    df = tweets_to_df(tweets)
    df.to_csv(f"s3://aaditya-twitter-etl/{username}_tweets.csv")
