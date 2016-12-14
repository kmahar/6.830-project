import requests
import json

def reformat_meetup(meetup_dict):
    return {
        'meetup_event_url' : meetup_dict['event_url'],
        'meetup_event_name' : meetup_dict['name'],
        'meetup_event_time' : meetup_dict['time'],
        'meetup_group_name' : meetup_dict['group']['name'],
        'meetup_group_lat' : meetup_dict['group']['group_lat'],
        'meetup_group_lon' : meetup_dict['group']['group_lon'],
        'meetup_venue_lat' : meetup_dict['venue']['lat'],
        'meetup_venue_lon' : meetup_dict['venue']['lon'],
        #'meetup_created_time' : datetime.datetime.fromtimestamp( meetup_dict['mtime'] / 1000.0),
        'meetup_created_time' : meetup_dict['mtime'],
        'meetup_id' : meetup_dict['id']
    }


def streamMeetups(output_file):
    r = requests.get('http://stream.meetup.com/2/open_events?since_mtime=1481642196000', stream=True)

    meetup_data = []
    try: 
        for line in r.iter_lines():
            meetup_dict = json.loads(line)
            try: 
                # only want US data, but can't filter with API request.
                if meetup_dict['venue']['country'] == 'us':
                    meetup_new = reformat_meetup(meetup_dict)
                    meetup_data.append(meetup_new)
            except KeyError:
                pass

            print len(meetup_data)

            if len(meetup_data) >= 15000:
                with open(output_file, 'w') as outf:
                    json.dump(meetup_data, outf)
                print "done!!"
                break

    except KeyboardInterrupt:
        with open(output_file, 'w') as outf:
            json.dump(meetup_data, outf)

streamMeetups('baseline_meetups.json')