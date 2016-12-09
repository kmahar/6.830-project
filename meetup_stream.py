import requests
import json
from datetime import datetime

r = requests.get('http://stream.meetup.com/2/open_events', stream=True)

for line in r.iter_lines():
	meetup_dict = json.loads(line)

	try: 
		meetup_new = {
			'event_url' : meetup_dict['event_url'],
			'group_name' : meetup_dict['group']['name'],
			'group_lat' : meetup_dict['group']['group_lat'],
			'group_lon' : meetup_dict['group']['group_lon'],
			'event_name' : meetup_dict['name'],
			'event_time' : meetup_dict['time'],
			'venue_lat' : meetup_dict['venue']['lat'],
			'venue_lon' : meetup_dict['venue']['lon'],
			'created_time' : datetime.fromtimestamp( meetup_dict['mtime'] / 1000.0),
			'id' : meetup_dict['id']
		}

	except KeyError:
		continue

	print meetup_new

