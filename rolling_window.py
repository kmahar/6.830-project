import pymongo
from pymongo import MongoClient
from vaderSentiment.vaderSentiment import sentiment as vaderSentiment 

client = MongoClient() #client = MongoClient('mongodb://localhost:27017/')
db = client.test
tweets = db.tweets

"""
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
"""

#return 50 buckets by state code
def calc_tweet_by_location():
    return

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

"""
#this version calculates the average sentiment over a group of hashtags
def calc_avg_sentiment_by_hashtags(hashtag_list):
    running_count = 0
    avg_sentiment = 0
    window = []
    window_size = 100

    #same process window function as before
    def process_window(avg_sentiment, window):
        avg_sentiment = (avg_sentiment*(running_count-len(window))+sum(sentiment["compound"] for sentiment in window))/(running_count*1.0)
        
        print running_count
        print avg_sentiment
        print

        return avg_sentiment

    for tweet in tweets.find():
        if("text" in tweet):
            for hashtag in tweet["entities"]["hashtags"]:
                if hashtag["text"].lower() in hashtag_list:
                    running_count += 1
                    print tweet["text"]
                    try:
                        tweetSentiment = vaderSentiment(tweet["text"])
                    except UnicodeEncodeError as e:
                        tweetSentiment = vaderSentiment(tweet["text"].encode("utf-8"))
                    print(tweetSentiment)
                    window.append(tweetSentiment)

        if len(window) >= window_size:
            avg_sentiment = process_window(avg_sentiment, window)
            window = []

    process_window(avg_sentiment, window)
"""

#See if something like #nevertrump has negative connotations, etc
#even people derailing a movement like #nevertrump express a negative sentiment,
#so this should be a reasonable test

#Note that for the average sentiment, you have to divide the
#sum of the sentiments (first return entry per hashtag) by the
#total number of sentiments (second return value per hashtag)
def calc_avg_sentiment_by_hashtag(hashtag_list):
    window = []
    window_size = 100
    #could/should probabaly combine the following two variables
    #lists the running count for this hashtag IN THE CURRENT WINDOW
    runningCountByHashtag = {hashtag: 0 for hashtag in hashtag_list}
    #lists the processed (sentiment sum, total hashtag count) for each hashtag
    #since I'm making this map to a tuple right now, it doesn't change well, 
    #which is why the often-updating runningCountByHashtag isn't included.
    sentimentByHashtag = {hashtag: (0,0) for hashtag in hashtag_list}

    def process_window(sentimentByHashtag, window):
        for hashtag in sentimentByHashtag:
            sentimentByHashtag[hashtag] = (sentimentByHashtag[hashtag][0]+sum(sentiment[1] for sentiment in window if sentiment[0]==hashtag), sentimentByHashtag[hashtag][1]+runningCountByHashtag[hashtag])
        
        #debugging only - remove in production
        print "{"
        for hashtag in sentimentByHashtag:
            print hashtag + ": [" + ", ".join(str(value) for value in sentimentByHashtag[hashtag]) + "], "
        print "}"

        return sentimentByHashtag

    for tweet in tweets.find():
        if("text" in tweet):
            for hashtag in tweet["entities"]["hashtags"]:
                hashtagText = hashtag["text"].lower()
                if hashtagText in hashtag_list:
                    runningCountByHashtag[hashtagText] = runningCountByHashtag[hashtagText]+1
                    print tweet["text"]
                    try:
                        tweetSentiment = vaderSentiment(tweet["text"])["compound"]
                    except UnicodeEncodeError as e:
                        tweetSentiment = vaderSentiment(tweet["text"].encode("utf-8"))["compound"]
                    print(tweetSentiment)
                    window.append((hashtagText, tweetSentiment))

        if len(window) >= window_size:
            sentimentByHashtag = process_window(sentimentByHashtag, window)
            window = []
            runningCountByHashtag = {hashtag: 0 for hashtag in hashtag_list}

    process_window(sentimentByHashtag, window)





"""print "CALCULATE AVG TIME"
calc_avg_time()
print """

print "CALCULATE AVG SENTIMENT"
calc_avg_sentiment()
print

hashtag_list = ["nevertrump", "spiritcooking", "draintheswamp"]
"""
print "CALCULATE AVG SENTIMENT GIVEN HASHTAGS: [" + ", ".join(hashtag_list) + "]"
calc_avg_sentiment_by_hashtags(hashtag_list)
print"""

print "CALCULATE AVG SENTIMENT BY HASHTAG: [" + ", ".join(hashtag_list) + "]"
calc_avg_sentiment_by_hashtag(hashtag_list)
print


