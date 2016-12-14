import time
import os
import io
import json
import requests

from tweepy import Stream
from tweepy import OAuthHandler
from tweepy.streaming import StreamListener
import datetime
from multiprocessing import Process, Queue, Value
from Queue import Empty
from stream_joiner import TweetStreamStore, MeetupStreamStore, StreamJoiner


ckey = '20bJqDsVLb649kmKBo0k28Dbe'
consumer_secret = 'aTBU47uIvTL5lyNsfQhD7obkM9XozcDqyyS1WzsXFgKz8TV0be'
access_token_key = '793668268540190720-o101NRo2TYknol7qJA6BhHSyXAVadH3'
access_token_secret = 'TOqP6kauz7RqnhfmVT2QiEyOLsyO0WTNHkRNfSMqLIOWo'

def reformat_tweet(tweet):
    return {
        'tweet_created_time' : datetime.datetime.fromtimestamp( int(tweet['timestamp_ms']) / 1000.0),
        'tweet_text' : tweet['text'],
        'tweet_username' : tweet['user']['screen_name'],
        'tweet_bounding_box_coords' : tweet['place']['bounding_box']['coordinates'][0]
    }

def reformat_meetup(meetup_dict):
    return {
        'meetup_event_url' : meetup_dict['event_url'],
        'meetup_event_name' : meetup_dict['name'],
        'meetup_event_time' : meetup_dict['time'],
        'meetup_group_name' : meetup_dict['group']['name'],
        'meetup_group_lat' : meetup_dict['group']['group_lat'],
        'meetup_group_lon' : meetup_dict['group']['group_lon'],
        'meetup_venue_lat' : meetup_dict['venue']['lat'],
        'meetup_venue_lon' : meetup_dict['venue']['lon'],
        'meetup_created_time' : datetime.datetime.fromtimestamp( meetup_dict['mtime'] / 1000.0),
        'meetup_id' : meetup_dict['id']
    }

def load_data(filename, stream_queue, delay):
    f = open(filename, 'r')
    data = json.load(f)
    f.close()
    for d in data:
        stream_queue.put(d)
        time.sleep(delay)


def streamTweets(stream_queue, counter):   

    start_time = time.time() #grabs the system time

    #Listener Class Override
    class listener(StreamListener):
     
        def __init__(self, start_time, time_limit=60):
            self.time = start_time
            self.limit = time_limit
     
        def on_data(self, data):
     
            if (time.time() - self.time) < self.limit:
                data = json.loads(data)
                try: 
                    new_data = reformat_tweet(data)
                    stream_queue.put(new_data)
                    with counter.get_lock():
                        counter.value += 1
                except (KeyError, TypeError):
                    pass

            else: 
                return False
     
        def on_error(self, status):
            print "Error: status code", status

    auth = OAuthHandler(ckey, consumer_secret) #OAuth object
    auth.set_access_token(access_token_key, access_token_secret)
    twitterStream = Stream(auth, listener(start_time, time_limit = 60*60*36)) 
    twitterStream.filter(languages=['en'], locations=[-124.0, 25.0, -67.0, 50.0])

def streamMeetups(stream_queue):
    r = requests.get('http://stream.meetup.com/2/open_events', stream=True)

    for line in r.iter_lines():
        meetup_dict = json.loads(line)
        try: 
            # only want US data, but can't filter with API request.
            if meetup_dict['venue']['country'] == 'us':
                meetup_new = reformat_meetup(meetup_dict)
                stream_queue.put(meetup_new)
        except KeyError:
            continue

if __name__ == "__main__":
    tweet_stream = Queue()
    meetup_stream = Queue()

    grid_width = 10
    grid_height = 10
    tweet_window_length = datetime.timedelta(minutes=1)
    meetup_window_length = datetime.timedelta(minutes=5)
    join_tolerance = 5 #in miles

    tweets = TweetStreamStore(grid_width, grid_height, tweet_window_length, join_tolerance)
    meetups = MeetupStreamStore(grid_width, grid_height, meetup_window_length, join_tolerance)

    joiner = StreamJoiner(tweets, meetups)
    output_queue = joiner.get_output_queue()

    # for actually streaming tweets
    # tweets_added_to_stream = Value('i', 0)
    # p1 = Process(target=streamTweets, args=(tweet_stream,tweets_added_to_stream))
    # p1.start()
    # p2 = Process(target=streamMeetups, args=(meetup_stream,))
    # p2.start()

    # for baseline testing, just load tweets from file instead 
    tweet_file = ''
    meetup_file = ''

    # if threads should sleep in between adding tweets to the queues 
    tweet_delay = 0
    meetup_delay = 0

    p1 = Process(target=load_data, args=(tweet_file, tweet_stream, tweet_delay))
    p2 = Process(target=load_data, args=(meetup_file, meetup_stream, meetup_delay))
    p1.start()
    p2.start()

    results = []

    while True:
        if not tweet_stream.empty():
            t = tweet_stream.get()
            joiner.add_item(1, t)

        if not meetup_stream.empty():
            m = meetup_stream.get()
            joiner.add_item(2, m)

        if not output_queue.empty():
            res = output_queue.get()
            results.append(res)

        print len(results)

        # change here to break when results reaches certain length? 