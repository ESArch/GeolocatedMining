import pymongo
from pymongo import errors
import datetime
from tweepy import TweepError
import geopandas as gpd
import time
import sys
from TimelineExtractor import Timeline

#provinces10 = gpd.read_file('/home/dieaigar/geodata/provinces10/ne_10m_admin_1_states_provinces.dbf')

client = pymongo.MongoClient('localhost', 27017)
db = client['twitter_db']
valencia = db['valencia_collection']
users_col = db['users']
seq_col = db['sequences']

tl = Timeline()
now = datetime.datetime.now()
last_week_ms = now.timestamp()*1000 - 604800000

count = 0
finished = False


while not finished:
    user_iterator = users_col.find({'touristLocal' : 'undefined' , 'last_review' : {"$lt" : last_week_ms}}).sort('last_review', pymongo.ASCENDING)

    if user_iterator.count() == 0:
        print("No more users to update, stopping process...")
        break

    try:
        for user in user_iterator:
            print("Updating data from {}".format(user['name']))
            last_sequence = None
            days_in_valencia = 0
            sequences = seq_col.find({'user_id' : user['id']}).sort("serial", pymongo.DESCENDING)
            print("Visits to Valencia so far...")
            for sequence in sequences:
                if last_sequence is None:
                    last_sequence = sequence
                    #print(last_sequence)

                first = datetime.datetime.fromtimestamp(sequence['first'])
                last = datetime.datetime.fromtimestamp(sequence['last'])
                print("From {} to {}".format(first, last))

                days_in_valencia += (last - first).days + 1
            print("He's been in Valencia for {} days".format(days_in_valencia))
            print()
            try:
                tl.timeline_since(last_sequence, days_in_valencia)
            except TweepError as e:
                print("TweepError: ", str(e))
                if "401" in e.args[0]:
                    checked = True
                    print("Error 401(Unauthorized): skipping this one")
                elif "404" in e.args[0]:
                    checked = True
                    print("Error 404(Non existant user: skipping this one")

            print()
    except errors.CursorNotFound as e:
        print("Cursor timeout, rebuilding the cursor...")
        user_iterator = users_col.find({'touristLocal': 'undefined', 'last_review': {"$lt": last_week_ms}})







