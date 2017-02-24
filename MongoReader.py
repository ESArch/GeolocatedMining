import pymongo
from bson import SON
import pandas as pd
import matplotlib.pyplot as plt
import json
from operator import itemgetter

client = pymongo.MongoClient('localhost', 27017)
db = client['twitter_db']
valencia = db['valencia_collection']
pois = db['poi']


#queryNear = {'coordinates' : SON([('$near' , {"type": "Point", "coordinates": [-0.3833, 39.4667]}) , ('$maxDistance' , 100 ) ]) }
#queryWithin = {'coordinates': {"$geoWithin" : {"$geometry" : poi['coordinates']}}}


for poi in pois.find().limit(1):
    query = {'coordinates': {"$geoWithin" : {"$geometry" : poi['coordinates']}}}
    tweets = valencia.find(query)
    if tweets.count() > 0:
        print("Tweets inside {}: {} ".format(poi['name'], tweets.count()))
        for tweet in tweets:
            print("@{} twitted: {}".format(tweet['user']['id'], tweet['text']))
        print()


tweets_iterator = valencia.find().sort('timestamp_ms', pymongo.ASCENDING)
f = open("tweetfile", 'w')

for tweet in tweets_iterator:
    f.write(str(tweet['id']) + '    ' + str(tweet['timestamp_ms']) + '\n')