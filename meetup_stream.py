import requests
import json
from datetime import datetime
from stream_joiner import *

window = datetime.timedelta(minutes=5)
meetups = MeetupStreamStore(5, 5, window, 5)

r = requests.get('http://stream.meetup.com/2/open_events', stream=True)

i = 0

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
			'meetup_created_time' : datetime.datetime.fromtimestamp( meetup_dict['mtime'] / 1000.0),
			'meetup_id' : meetup_dict['id']
		}

		meetups.add(meetup_new)
		i += 1
		print meetups.cells

		if i == 100:
			break

	except KeyError:
		continue


