import pymongo
from pymongo import MongoClient
from vaderSentiment.vaderSentiment import sentiment as vaderSentiment 

client = MongoClient() #client = MongoClient('mongodb://localhost:27017/')
db = client.test
tweets = db.tweets

#Simple testing function
#All results should be roughly equal, but we can't guarantee increasing/decreasing
def calc_avg_time():
    running_count = 0
    avg_time = 0
    window = []
    window_size = 100

    def process_window(avg_time, window):
        avg_time = (avg_time*(running_count-len(window))+sum(window))/(running_count*1.0)

        print running_count
        print avg_time
        print

        return avg_time

    #rather than using a for loop, we want a subscriber model window
    #that will perform the proper processing when complete
    for tweet in tweets.find():
        if("timestamp_ms" in tweet):
            running_count += 1
            window.append(int(tweet["timestamp_ms"]))
        if len(window) >= window_size:
            avg_time = process_window(avg_time, window)
            window = []

    process_window(avg_time, window)

#Similar to previous function, but now using vaderSentiment
#**Hutto, C.J. & Gilbert, E.E. (2014). VADER: A Parsimonious Rule-based Model for Sentiment Analysis of Social Media Text. Eighth International Conference on Weblogs and Social Media (ICWSM-14). Ann Arbor, MI, June 2014.** 
#Admittedly, the general sentiment over a lot of tweets isn't THAT useful
def calc_avg_sentiment():
    running_count = 0
    avg_sentiment = 0
    window = []
    window_size = 100

    def process_window(avg_sentiment, window):
        avg_sentiment = (avg_sentiment*(running_count-len(window))+sum(sentiment["compound"] for sentiment in window))/(running_count*1.0)
        
        print running_count
        print avg_sentiment
        print

        return avg_sentiment

    for tweet in tweets.find():
        if("text" in tweet):
            running_count += 1
            try:
                window.append( vaderSentiment(tweet["text"]) )
            except UnicodeEncodeError as e:
                window.append( vaderSentiment(tweet["text"].encode("utf-8")) )
        if len(window) >= window_size:
            avg_sentiment = process_window(avg_sentiment, window)
            window = []

    process_window(avg_sentiment, window)

def calc_avg_sentiment_by_hashtag():
    return



print "CALCULATE AVG TIME"
calc_avg_time()
print 

print "CALCULATE AVG SENTIMENT"
calc_avg_sentiment()
print


