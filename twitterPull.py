import time
from tweepy import Stream
from tweepy import OAuthHandler
from tweepy.streaming import StreamListener
import os
import io

ckey = ''
consumer_secret = ''
access_token_key = ''
access_token_secret = ''


start_time = time.time() #grabs the system time

def load_keywords(filename):
    f = open(filename, 'r')
    terms = [keyword.strip() for keyword in list(f)]
    f.close()
    return terms

#Listener Class Override
class listener(StreamListener):
 
    def __init__(self, start_time, time_limit=60):
 
        self.time = start_time
        self.limit = time_limit
        self.tweet_data = []
        self.saveFile = io.open('raw_tweets.json', 'w', encoding='utf-8')
        self.saveFile.write(u'[\n')
 
    def on_data(self, data):
 
        if (time.time() - self.time) < self.limit:
            self.tweet_data.append(data)
            if len(self.tweet_data) == 1000:
                print "dumping tweets into json file"
                self.write_data()
                self.tweet_data = []
            return True
        self.write_data()
        self.saveFile.write(u'\n]')
        self.saveFile.close()
        return False

    def write_data(self):
        self.saveFile.write(','.join(self.tweet_data))
 
    def on_error(self, status):
        print status

auth = OAuthHandler(ckey, consumer_secret) #OAuth object
auth.set_access_token(access_token_key, access_token_secret)

twitterStream = Stream(auth, listener(start_time, time_limit = 60*60*36)) #initialize Stream object with a time out limit
#first four numbers = US, second four numbers = Alaska, last four numbers = Hawaii (southwest first, then northeast)
#call the filter method to run the Stream Object
twitterStream.filter(track=load_keywords('keywords.txt'), languages=['en'], locations=[-126.185404, 25.447902, -61.538415, 49.844730, -167.797322, 52.300594, -140.596922, 72.202104, -160.799852, 18.225603, -154.800269, 22.239597])


