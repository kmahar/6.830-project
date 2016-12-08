import requests

r = requests.get('http://stream.meetup.com/2/open_events', stream=True)

for line in r.iter_lines():
	print line