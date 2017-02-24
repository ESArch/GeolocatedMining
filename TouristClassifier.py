import pymongo
import geopandas as gpd
from TimelineExtractor import Timeline

provinces10 = gpd.read_file('/home/dieaigar/geodata/provinces10/ne_10m_admin_1_states_provinces.dbf')

client = pymongo.MongoClient('localhost', 27017)
db = client['twitter_db']
valencia = db['valencia_collection']
pois = db['poi']
users = db['users']

limit = 100
finished = False
tl = Timeline()

f = open("tweetfile", 'w')

while not finished:

    last_timestamp = open('last_timestamp', 'r').read()
    query = {'timestamp_ms': {"$gt": last_timestamp}}
    tweets_iterator = valencia.find(query).sort('timestamp_ms', pymongo.ASCENDING).limit(limit)

    if tweets_iterator.count() < limit:
        finished = True

    for tweet in tweets_iterator:
        potential_tourist = False

        # Check if we already know this user
        known_user = users.find_one({'id': tweet['user']['id']})
        if known_user:
            # If we already know him skip his tweet
            # DECIDE IF WE SHOULD UPDATE HIS TWEETS AT THIS POINT
            continue
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

        open('last_timestamp', 'w').write(tweet['timestamp_ms'])

print(valencia.find().count())


