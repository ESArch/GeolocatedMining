import pymongo
import datetime
import geopandas as gpd
from TimelineExtractor import Timeline
from shapely.geometry import mapping, shape

client = pymongo.MongoClient('localhost', 27017)
db = client['twitter_db']
valencia = db['valencia_collection']
pois = db['poi']
users = db['users']

provinces10 = gpd.read_file('/home/dieaigar/geodata/provinces10/ne_10m_admin_1_states_provinces.dbf')

f = open('last_timestamp', 'r')
last_timestamp = f.read()
last_timestamp = eval(last_timestamp)

new_timestamp = datetime.datetime.now().timestamp()


tl = Timeline()

new_tweets = valencia.find({'timestamp_ms' : {"$gt" : str(last_timestamp*1000), "$lte" : str(new_timestamp*1000)}}).limit(10)

for tweet in new_tweets.sort("timestamp_ms", 1):
    potential_tourist = False

    # Check if we already know this user
    known_user = users.find_one({'id' : tweet['user']['id']})
    if known_user:
        # If we already know him skip his tweet
        # DECIDE IF WE SHOULD UPDATE HIS TWEETS AT THIS POINT
        break
        print('known user')
    else:
        # If we don't know him check if he uses a different language
        # or tweets from POIs

        potential_tourist = tweet['user']['lang'] != 'es'

        if not potential_tourist:
            for poi in pois.find():
                query = {'id': tweet['id'], 'coordinates': {"$geoWithin": {"$geometry": poi['coordinates']}}}
                tweets = valencia.find(query)
                if tweets.count() > 0:
                    print('@{} twitted from {}'.format(tweet['user']['name'], poi['name']))
                    potential_tourist = True
                    break

    if potential_tourist:
        print('Extraer timeline de @{} para identificar tipo de usuario'.format(tweet['user']['name']))
        tl.timeline_until(tweet['user']['id'], tweet['id'])






'''
for poi in pois.find():
    query = {'coordinates': {"$geoWithin" : {"$geometry" : poi['coordinates']}}}
    tweets = valencia.find(query)
    if tweets.count() > 0:
        print("Tweets inside {}: {} ".format(poi['name'], tweets.count()))
        for tweet in tweets:
            print("@{} twitted: {}".format(tweet['user']['id'], tweet['text']))
        print()
'''