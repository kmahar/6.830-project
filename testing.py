import time
import json
import cProfile
import datetime
import Queue
from stream_joiner import TweetStreamStore, MeetupStreamStore, StreamJoiner
from geopy.distance import vincenty 

def load_data_to_queue(filename, stream_queue):
    f = open(filename, 'r')
    data = json.load(f)
    f.close()
    for d in data[:1000]:
        if 'meetup_created_time' in d:
            d['meetup_created_time'] = datetime.datetime.fromtimestamp( int(d['meetup_created_time']) / 1000.0)
        elif 'tweet_created_time' in d:
            d['tweet_created_time'] = datetime.datetime.fromtimestamp( int(d['tweet_created_time']) / 1000.0)
        stream_queue.put(d)

    print "done loading data from %s" % filename

def load_data_to_list(filename):
    result = []
    f = open(filename, 'r')
    data = json.load(f)
    f.close()
    for d in data[:1000]:
        if 'meetup_created_time' in d:
            d['meetup_created_time'] = datetime.datetime.fromtimestamp( int(d['meetup_created_time']) / 1000.0)
        elif 'tweet_created_time' in d:
            d['tweet_created_time'] = datetime.datetime.fromtimestamp( int(d['tweet_created_time']) / 1000.0)
        result.append(d)

    return result

def close_enough(coords1, coords2):
    dist = vincenty(coords1, coords2).miles
    return dist <= 5

def get_avg_location(t):
    tweet_bounding_box = t['tweet_bounding_box_coords']
    lats = [coord[1] for coord in tweet_bounding_box]
    lons = [coord[0] for coord in tweet_bounding_box]
    avgLat = sum(lats) / len(lats)
    avgLon = sum(lons) / len(lons)
    return (avgLat, avgLon)

def join_items(i1, i2):
    new_item = {}
    new_item.update(i1)
    new_item.update(i2)
    return new_item

def do_brute_force(tweets, meetups):
    result = []
    for t in tweets:
        t_coords = get_avg_location(t) 
        for m in meetups:
            m_coords = m['meetup_venue_lat'], m['meetup_venue_lon']
            if close_enough(t_coords, m_coords):
                result.append(join_items(t, m))
    print "number of joins:", len(result)
    return result

def do_test(q1, q2, joiner):

    while not q1.empty() or not q2.empty():
        if not q1.empty():
            d = q1.get()
            joiner.add_item(1, d)

        if not q2.empty():
            d = q2.get()
            joiner.add_item(2, d)


if __name__ == "__main__":

    # basic 
    meetup_queue = Queue.Queue()
    tweet_queue = Queue.Queue()
    load_data_to_queue('meetup_baseline_data.json', meetup_queue)
    load_data_to_queue('tweet_baseline_data.json', tweet_queue)

    grid_width = 500
    grid_height = 350
    tweet_window_length = datetime.timedelta(days=5)
    meetup_window_length = datetime.timedelta(days=5)
    join_tolerance = 5 #in miles

    tweets = TweetStreamStore(grid_width, grid_height, tweet_window_length, join_tolerance)
    meetups = MeetupStreamStore(grid_width, grid_height, meetup_window_length, join_tolerance)
    joiner = StreamJoiner(tweets, meetups)

    cProfile.run('do_test(tweet_queue, meetup_queue, joiner)')

    output = joiner.get_output_queue()
    num_joined = len(output.queue)
    print "produced %s joins" % num_joined


    # results = []
    # while not output.empty():
    #     d = output.get()
    #     d_new = {
    #         'meetup_id' : d['meetup_id'],
    #         'tweet_text' : d['tweet_text'],
    #         'meetup_venue_lon': d['meetup_venue_lon'],
    #         'meetup_venue_lat': d['meetup_venue_lat'],
    #         'tweet_bounding_box_coords' : d['tweet_bounding_box_coords']
    #     }
    #     results.append(d_new)

    # f = open('expected_results.json', 'r')
    # expected_data = json.load(f)
    # f.close()

    # e_set = set([str(e) for e in expected_data])
    # r_set = set([str(r) for r in results])

    # print "e set:", len(e_set)
    # print "r set:", len(r_set)

    # for e in expected_data:
    #     if e not in results:
    #         print e
