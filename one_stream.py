import time
import os
import io
import json
import requests

from tweepy import Stream
from tweepy import OAuthHandler
from tweepy.streaming import StreamListener
import datetime
from multiprocessing import Process, Queue
from Queue import Empty
from windows_class import Aggregator, Window


text_file = open("data.txt")
lines = text_file.read().split('\n')
text_file.close()

# sd = dict(u.split(":") for u in lines.split(","))

a = 1
for line in lines:
    print a
    print line
    print line.split()


    a = a+1
    # sd = dict(u.split(":") for u in lines.split(","))
    # print sd
    # (key, val) = line.split()
    # d[int(key)] = val
    # print line
    # print key
    # print val
# print d




# for text in lines:
#     meetup_dict = json.loads(line)
#     print text

# if __name__ == "__main__":
#     stream1 = Queue()
#     stream2 = Queue()

#     grid_width = 10
#     grid_height = 10
#     window_length = datetime.timedelta(minutes=1)
#     join_tolerance = 5

#     tweets = TweetStreamStore(grid_width, grid_height, window_length, join_tolerance)
#     meetups = MeetupStreamStore(grid_width, grid_height, window_length, join_tolerance)

#     joiner = StreamJoiner(tweets, meetups)

#     output_queue = joiner.get_output_queue()

#     p1 = Process(target=streamTweetFile, args=(stream1,))
#     p1.start()
#     p2 = Process(target=streamMeetupFile, args=(stream2,))
#     p2.start()

#     while True:
#         try:
#             res = stream1.get_nowait()
#             joiner.add_item(1, res)
#         except Empty:
#             pass
#         try:
#             res = stream2.get_nowait()
#             joiner.add_item(2, res)
#         except Empty:
#             pass
#         try:  
#             res = output_queue.get_nowait()
#             print res
#         except Empty:
#             pass

# # if __name__ == "__main__":
# #     execfile("two_streams.py")

#     # --- examples -------
# sentences = ["VADER is smart, handsome, and funny.",      # positive sentence example
#             "VADER is not smart, handsome, nor funny.",   # negation sentence example
#             "VADER is smart, handsome, and funny!",       # punctuation emphasis handled correctly (sentiment intensity adjusted)
#             "VADER is very smart, handsome, and funny.",  # booster words handled correctly (sentiment intensity adjusted)
#             "VADER is VERY SMART, handsome, and FUNNY.",  # emphasis for ALLCAPS handled
#             "VADER is VERY SMART, handsome, and FUNNY!!!",# combination of signals - VADER appropriately adjusts intensity
#             "VADER is VERY SMART, uber handsome, and FRIGGIN FUNNY!!!",# booster words & punctuation make this close to ceiling for score
#             "The book was good.",                                     # positive sentence
#             "The book was kind of good.",                 # qualified positive sentence is handled correctly (intensity adjusted)
#             "The plot was good, but the characters are uncompelling and the dialog is not great.", # mixed negation sentence
#             "At least it isn't a horrible book.",         # negated negative sentence with contraction
#             "Make sure you :) or :D today!",              # emoticons handled
#             "Today SUX!",                                 # negative slang with capitalization emphasis
#             "Today only kinda sux! But I'll get by, lol"  # mixed sentiment example with slang and constrastive conjunction "but"
#              ]

# analyzer = SentimentIntensityAnalyzer()
# for sentence in sentences:
#     vs = analyzer.polarity_scores(sentence)
#     print("{:-<65} {}".format(sentence, str(vs)))

# Read in pairs (meetup, tweet)
# Build dictionary of {(m1_lat, m1_lon):(t1, t2, ...), (m2_lat, m2_lon):(t1, t2, ...), ...}
    # If current location is not in dictionary, add to dictionary
    # Else, add to window
# Refresh window averages and display on map