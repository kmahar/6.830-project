import time
from tweepy import Stream
from tweepy import OAuthHandler
from tweepy.streaming import StreamListener
import os
import io

ckey = '3Juvs6OcXBOeyJuy12h6QjEKl'
consumer_secret = 'tOjVfBa1dGNcHBvMCw2Pdwj6CGP3f36vScDbU3alFMCMSeFf0J'
access_token_key = '793668268540190720-o101NRo2TYknol7qJA6BhHSyXAVadH3'
access_token_secret = 'TOqP6kauz7RqnhfmVT2QiEyOLsyO0WTNHkRNfSMqLIOWo'


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
 
	def on_data(self, data):
 
		saveFile = io.open('raw_tweets.json', 'a', encoding='utf-8')
 
		while (time.time() - self.time) < self.limit:
 
			try:
 
				self.tweet_data.append(data)
 
				return True
 
 
			except BaseException, e:
				print 'failed ondata,', str(e)
				time.sleep(5)
				pass
 
		saveFile = io.open('raw_tweets.json', 'w', encoding='utf-8')
		saveFile.write(u'[\n')
		saveFile.write(','.join(self.tweet_data))
		saveFile.write(u'\n]')
		saveFile.close()
		exit()
 
	def on_error(self, status):
 
		print statuses

auth = OAuthHandler(ckey, consumer_secret) #OAuth object
auth.set_access_token(access_token_key, access_token_secret)


twitterStream = Stream(auth, listener(start_time, time_limit=20)) #initialize Stream object with a time out limit

#first four numbers = US, second four numbers = Alaska, last four numbers = Hawaii (southwest first, then northeast)
twitterStream.filter(track=load_keywords('keywords.txt'), languages=['en'], locations=[-126.185404, 25.447902, -61.538415, 49.844730, -167.797322, 52.300594, -140.596922, 72.202104, -160.799852, 18.225603, -154.800269, 22.239597])  #call the filter method to run the Stream Object