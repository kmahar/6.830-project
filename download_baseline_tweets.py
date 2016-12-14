import datetime
import time
from tweepy import Stream, OAuthHandler
from tweepy.streaming import StreamListener
import json

ckey = '20bJqDsVLb649kmKBo0k28Dbe'
consumer_secret = 'aTBU47uIvTL5lyNsfQhD7obkM9XozcDqyyS1WzsXFgKz8TV0be'
access_token_key = '793668268540190720-o101NRo2TYknol7qJA6BhHSyXAVadH3'
access_token_secret = 'TOqP6kauz7RqnhfmVT2QiEyOLsyO0WTNHkRNfSMqLIOWo'


def reformat_tweet(tweet):
    return {
        'tweet_created_time' : tweet['timestamp_ms'],
        #'tweet_created_time' : datetime.datetime.fromtimestamp( int(tweet['timestamp_ms']) / 1000.0),
        'tweet_text' : tweet['text'],
        'tweet_username' : tweet['user']['screen_name'],
        'tweet_bounding_box_coords' : tweet['place']['bounding_box']['coordinates'][0]
    }

#Listener Class Override
class listener(StreamListener):
 
    def __init__(self, start_time, time_limit=60):
        self.time = start_time
        self.limit = time_limit
        self.tweets = []
 
    def on_data(self, data):
  
        if (time.time() - self.time) < self.limit:
            data = json.loads(data)
            try: 
                new_data = reformat_tweet(data)
                self.tweets.append(new_data)
            except (KeyError, TypeError):
                pass

        else: 
            with open('baseline_tweets.json', 'w') as outf:
                json.dump(self.tweets, outf)
            return False
 
    def on_error(self, status):
        print "Error: status code", status

auth = OAuthHandler(ckey, consumer_secret) #OAuth object
auth.set_access_token(access_token_key, access_token_secret)
start_time = time.time()
twitterStream = Stream(auth, listener(start_time, time_limit = 10*60)) 
twitterStream.filter(languages=['en'], locations=[-124.0, 25.0, -67.0, 50.0])