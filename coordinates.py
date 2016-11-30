import json
from tweepy import Stream
from tweepy import OAuthHandler
from tweepy.streaming import StreamListener

#Enter Twitter API Key information
consumer_key = ''
consumer_secret = ''
access_token = ''
access_secret = ''

file = open("C:\\Output.csv", "w")
file.write("X,Y\n")

data_list = []
count = 0

class listener(StreamListener):

    def on_data(self, data):
        global count

        #How many tweets you want to find, could change to time based
        if count <= 2000:
            json_data = json.loads(data)

            coords = json_data["coordinates"]
            if coords is not None:
               print coords["coordinates"]
               lon = coords["coordinates"][0]
               lat = coords["coordinates"][1]

               data_list.append(json_data)

               file.write(str(lon) + ",")
               file.write(str(lat) + "\n")

               count += 1
            return True
        else:
            file.close()
            return False

    def on_error(self, status):
        print status

auth = OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_secret)
twitterStream = Stream(auth, listener())
#What you want to search for here
twitterStream.filter(track=["Halloween"])