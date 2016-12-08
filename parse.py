import json
import os

# parses tweet file and returns list of dictionaries, where 
# each element in the list corresponds to one tweet. 
def parseTweets(infile):
	errorCount = 0
	parsed_tweets = []
	i = 0
	with open(infile, 'r') as f:
		for t in f:
			if t.startswith('[') or t.startswith(','):
				t = t[1:]
			if t.endswith(']'):
				t = t[:-1]
			if len(t) < 2:
				continue
			parsed = json.loads(t)
			parsed_tweets.append(parsed)
			i += 1
			print i

	print "done"
	return parsed_tweets

parseTweets('/Users/kaitlinmahar/Dropbox (MIT)/copy/raw_tweets_2.json')