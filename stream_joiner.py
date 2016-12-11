import Queue
import collections
import datetime
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
		self.size = 0

		# hard code these for now... rough contiguous US boundaries
		self.min_x = -124.0
		self.max_x = -67.0
		self.min_y = 25.0
		self.max_y = 50.0

		self.x_box_size = (self.max_x - self.min_x) / self.width 
		self.y_box_size = (self.max_y - self.min_y) / self.height 

	def get_size(self):
		return self.size

	def add(self, item):
		x, y = self.hash(item)
		if (x,y) in self.cells:
			self.cells[(x,y)].append(item)
			self.size += 1

	def get_joins(self, other_stream_item):
		
		joins = []		
		x, y = self.other_hash(other_stream_item)

		surrounding = [(x-1, y-1), (x-1, y), (x-1, y+1), 
						(x, y-1), (x, y), (x, y+1), 
						(x+1, y-1), (x+1, y), (x+1, y+1)]	
			
		for s in surrounding:
			s_x, s_y = s
			if (s_x, s_y) in self.cells:
				
				self.delete_expired(s_x, s_y)

				for c in self.cells[(s_x,s_y)]:
					if self.should_join(c, other_stream_item):
						joins.append(self.join_items(c, other_stream_item))
		
		return joins

	def delete_expired(self, x, y):
		if (x,y) not in self.cells:
			return

		current_items = self.cells[(x, y)]

		# remove data until first one is current
		while len(current_items) > 0:
			if not self.is_expired(current_items[0]):
				break
			current_items.popleft()
			self.size -= 1

		self.cells[(x, y)] = current_items

	def is_expired(self, item):
		return self.get_time(item) + self.window_length < datetime.datetime.now()

	def join_items(self, this_stream_item, other_stream_item):
		# combine all keys and return new dict. 
		new_item = {}
		new_item.update(this_stream_item)
		new_item.update(other_stream_item)
		return new_item

	def hash_lat_lon(self, lat, lon):
		y_dist_from_start = lat - self.min_y
		x_dist_from_start = lon - self.min_x
		x_box = int(math.floor(x_dist_from_start / self.x_box_size))
		y_box = int(math.floor(y_dist_from_start / self.y_box_size))
		return (x_box, y_box)

	# avg lat and lon from a bounding box 
	def get_avg_location(self, item):
		tweet_bounding_box = item['tweet_bounding_box_coords']
		lats = [coord[1] for coord in tweet_bounding_box]
		lons = [coord[0] for coord in tweet_bounding_box]
		avgLat = sum(lats) / len(lats)
		avgLon = sum(lons) / len(lons)
		return (avgLat, avgLon)

	def close_enough(self, coords1, coords2):
		dist = vincenty(coords1, coords2).miles
		return dist <= self.join_tolerance

	#### to be implemented in child classes
	def hash(self, item):
		raise NotImplementedError
	#### to be implemented in child classes
	def other_hash(self, other_stream_item):
		raise NotImplementedError
	#### to be implemented in child classes
	def get_time(self, item):
		raise NotImplementedError
	#### to be implemented in child classes
	def should_join(self, this_stream_item, other_stream_item):
		raise NotImplementedError

class TweetStreamStore(GridStreamStore):

	def add(self, item):
		GridStreamStore.add(self, item)
		x, y = self.hash(item)
		self.delete_expired(x, y)

	# check if a meetup should be joined to a tweet in this stream
	def should_join(self, this_stream_item, other_stream_item):
		tweet_coords = self.get_avg_location(this_stream_item)
		meetup_coords = (other_stream_item['meetup_venue_lat'], other_stream_item['meetup_venue_lon'])
		return self.close_enough(tweet_coords, meetup_coords)

	# hash a tweet by its location (same as MeetupStreamStore.other_hash)
	def hash(self, item):
		lat, lon = self.get_avg_location(item)
		return self.hash_lat_lon(lat, lon)

	# hash a meetup by its location (same as MeetupStreamStore.hash)
	def other_hash(self, other_stream_item):
		lat, lon = other_stream_item['meetup_venue_lat'], other_stream_item['meetup_venue_lon']
		return self.hash_lat_lon(lat, lon)

	# get the time from a tweet 
	def get_time(self, item):
		return item['tweet_created_time']


class MeetupStreamStore(GridStreamStore):

	# check if a tweet should be joined to a meetup in this stream
	def should_join(self, this_stream_item, other_stream_item):
		tweet_coords = self.get_avg_location(other_stream_item)
		meetup_coords = (this_stream_item['meetup_venue_lat'], this_stream_item['meetup_venue_lon'])
		return self.close_enough(tweet_coords, meetup_coords)

	#  hash a meetup by its location (same as TwitterStreamStore.other_hash)
	def hash(self, item):
		lat, lon = item['meetup_venue_lat'], item['meetup_venue_lon']
		return self.hash_lat_lon(lat, lon)

	# hash a tweet by its location (same as TwitterStreamStore.hash)
	def other_hash(self, other_stream_item):
		lat, lon = self.get_avg_location(other_stream_item)
		return self.hash_lat_lon(lat, lon)

	# get the time from a meetup
	def get_time(self, item):
		return item['meetup_created_time']