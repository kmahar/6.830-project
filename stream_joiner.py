import Queue
import collections
from datetime import datetime, timedelta
import math
from geopy.distance import vincenty 

class StreamJoiner(object):

	'''
	input: stream_stores - a list of StreamStores 
	'''
	def __init__(self, stream_1, stream_2):
		self.stream_1 = stream_1
		self.stream_2 = stream_2 
		self.output_queue = Queue.Queue() # this is synchronized!

	'''
	input: stream_number - index corresponding to which stream 
			in stream_stores should have item added to it.
	'''
	def add_item(self, stream_number, item):

		if stream_number == 1:
			other_stream = self.stream_2
			self.stream_1.add(item)
		elif stream_number == 2:
			other_stream = self.stream_1
			self.stream_2.add(item)

		results = other_stream.get_joins(item)
		self.add_output(results)

	def add_output(self, output_list):
		for o in output_list:
			self.output_queue.put(o)

	def get_output_queue(self):
		return self.output_queue


class GridStreamStore(object):

	def __init__(self, grid_width, grid_height, window_length, join_tolerance):
		self.width = grid_width
		self.height = grid_height
		self.cells = {(i, j) : collections.deque() for i in range(self.width) for j in range(self.height)}
		self.window_length = window_length
		self.join_tolerance = join_tolerance # in miles 

		# hard code these for now... rough contiguous US boundaries
		self.min_x = -124.0
		self.max_x = -67.0
		self.min_y = 25.0
		self.max_y = 50.0

		self.x_box_size = (self.max_x - self.min_x) / width 
		self.y_box_size = (self.max-y - self.min_y) / height 


	def add(self, item):
		x, y = self.hash(item)
		self.cells[(x,y)].append(item)

	def get_joins(self, other_stream_item):
		
		joins = []		
		x, y = self.other_hash(item)

		surrounding = [(x-1, y-1), (x-1, y), (x-1, y+1), 
						(x, y-1), (x, y), (x, y+1), 
						(x+1, y-1), (x+1, y), (x+1, y+1)]	
			
		for s in surrounding:
			s_x, s_y = s
			if (s_x, s_y) in self.cells:
				current_items = self.cells[(s_x, s_y)]

				# remove data until first one is current
				while len(current_items) > 0:
					if not self.is_expired(current_items[0]):
						break
					current_items.popleft()

				self.cells[(s_x, s_y)] = current_items

				for c in current_items:
					if self.should_join(c, item):
						joins.append(self.join_items(c, item))
		
		return joins

	def is_expired(self, item):
		return self.get_time(item) + self.window_length < datetime.now()

	def should_join(self, this_stream_item, other_stream_item):
		raise NotImplementedError

	def join_items(self, this_stream_item, other_stream_item):
		new_item = {}
		new_item.update(this_stream_item)
		new_item.update(other_stream_item)
		return new_item

	def hash(self, item):
		raise NotImplementedError

	def other_hash(self, other_stream_item):
		raise NotImplementedError

	def get_time(self, item):
		raise NotImplementedError

class TweetStreamStore(GridStreamStore):

	# check if a meetup should be joined to a tweet in this stream
	def should_join(self, this_stream_item, other_stream_item):
		tweet_coords = self.get_avg_location(this_stream_item)
		meetup_coords = (other_stream_item['venue_lat'], other_stream_item['venue_lon'])
		dist = vincenty(tweet_coords, meetup_coords).kilometers

	# hash a tweet by its location (same as MeetupStreamStore.other_hash)
	def hash(self, item):
		lat, lon = self.get_avg_location(item)

	# hash a meetup by its location (same as MeetupStreamStore.hash)
	def other_hash(self, other_stream_item):
		return MeetupStreamStore.hash(self, other_stream_item)

	# get the time from a tweet 
	def get_time(self, item):
		raise NotImplementedError

	def get_avg_location(self, item):
		tweet_bounding_box = item['bounding_box_coords']
		lats = set()
		lons = set()
		for coord in tweet_bounding_box:
			lons.add(coord[0])
			lats.add(coord[1])

		avgLat = sum(lats) / len(lats)
		avgLon = sum(lons) / len(lons)

		return (avgLat, avgLon)

class MeetupStreamStore(GridStreamStore):

	# check if a tweet should be joined to a meetup in this stream
	def should_join(self, this_stream_item, other_stream_item):
		raise NotImplementedError

	#  hash a meetup by its location (same as TwitterStreamStore.other_hash)
	def hash(self, item):
		raise NotImplementedError

	# hash a tweet by its location (same as TwitterStreamStore.hash)
	def other_hash(self, other_stream_item):
		return TweetStreamStore.hash(self, other_stream_item)

	# get the time from a meetup
	def get_time(self, item):
		raise NotImplementedError
