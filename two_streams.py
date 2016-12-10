from multiprocessing import Process, Queue
from Queue import Empty

import time
from tweepy import Stream
from tweepy import OAuthHandler
from tweepy.streaming import StreamListener
import os
import io
from datetime import datetime
import json

import requests

#pretty much twitterPull.py
#should be externally called, but that was taking me too long to figure out
def streamTweetFile(stream_queue):   
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
     
        def on_data(self, data):
     
            if (time.time() - self.time) < self.limit:
                # reformat the twitter data
                data = json.loads(data)

                try: 
                    new_data = {
                        'tweet_created_time' : datetime.fromtimestamp( int(data['timestamp_ms']) / 1000.0),
                        'tweet_text' : data['text'],
                        'tweet_username' : data['user']['screen_name'],
                        'tweet_bounding_box_coords' : data['place']['bounding_box']['coordinates'][0]
                    }
                    stream_queue.put(new_data)

                except KeyError:
                    pass
            else: 
                return False

        # def write_data(self):
        #     self.saveFile.write(','.join(self.tweet_data))
     
        def on_error(self, status):
            print status

    auth = OAuthHandler(ckey, consumer_secret) #OAuth object
    auth.set_access_token(access_token_key, access_token_secret)

    twitterStream = Stream(auth, listener(start_time, time_limit = 60*60*36)) #initialize Stream object with a time out limit
    #first four numbers = US, second four numbers = Alaska, last four numbers = Hawaii (southwest first, then northeast)
    #call the filter method to run the Stream Object
    twitterStream.filter(languages=['en'], locations=[-126.185404, 25.447902, -61.538415, 49.844730, -167.797322, 52.300594, -140.596922, 72.202104, -160.799852, 18.225603, -154.800269, 22.239597])

#pretty much meetup_stream.py
#should be externally called, but that was taking me too long to figure out
def streamMeetupFile(stream_queue):
    r = requests.get('http://stream.meetup.com/2/open_events', stream=True)

    for line in r.iter_lines():
        meetup_dict = json.loads(line)

        try: 
            if meetup_dict['group']['country'] != 'us' or meetup_dict['venue']['country'] != 'us':
                continue

            meetup_new = {
                'meetup_event_url' : meetup_dict['event_url'],
                'meetup_group_name' : meetup_dict['group']['name'],
                'meetup_group_lat' : meetup_dict['group']['group_lat'],
                'meetup_group_lon' : meetup_dict['group']['group_lon'],
                'meetup_event_name' : meetup_dict['name'],
                'meetup_event_time' : meetup_dict['time'],
                'meetup_venue_lat' : meetup_dict['venue']['lat'],
                'meetup_venue_lon' : meetup_dict['venue']['lon'],
                'meetup_created_time' : datetime.fromtimestamp( meetup_dict['mtime'] / 1000.0),
                'meetup_id' : meetup_dict['id']
            }

        except KeyError:
            continue

        stream_queue.put(meetup_new)



if __name__ == "__main__":
    stream1 = Queue()
    stream2 = Queue()

    p1 = Process(target=streamTweetFile, args=(stream1,))
    p1.start()
    p2 = Process(target=streamMeetupFile, args=(stream2,))
    p2.start()

    while True:
        print
        print "Size of stream 1:"
        print stream1.get()
        print "size of stream 2:"
        print stream2.get()
        print 
        time.sleep(1)