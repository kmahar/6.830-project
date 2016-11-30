import pymongo
from geopy.geocoders import Nominatim

client = pymongo.MongoClient('mongodb://localhost:27017/')
db = client.test
tweets = db.tweets

LOCATOR = Nominatim()

def getState(boundingBoxCoords):
	lats = set()
	lons = set()
	for c in boundingBoxCoords:
		lons.add(c[0])
		lats.add(c[1])

	assert(len(lats) == 2)
	assert(len(lons) == 2)

	avgLat = sum(lats) / 2.0
	avgLon = sum(lons) / 2.0

	coordString = "%s, %s" % (avgLat, avgLon)

	raw_response = LOCATOR.reverse(coordString).raw
	try:
		return raw_response['address']['state']
	except KeyError:
		print raw_response

sampleTweets = tweets.find({"place": {'$ne': None}}, {"place.bounding_box.coordinates": 1}).limit(100)

stateCounts = {}

for t in sampleTweets:
	coords = t['place']['bounding_box']['coordinates'][0]
	state = getState(coords)
	stateCounts[state] = stateCounts.get(state, 0) + 1

print stateCounts