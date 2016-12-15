import pymongo
from bson import SON
import pandas as pd
import matplotlib.pyplot as plt
import json

client = pymongo.MongoClient('localhost', 27017)
db = client['twitter_db']
valencia = db['valencia_collection']
pois = db['poi']


#queryNear = {'coordinates' : SON([('$near' , {"type": "Point", "coordinates": [-0.3833, 39.4667]}) , ('$maxDistance' , 100 ) ]) }
#queryWithin = {'coordinates': {"$geoWithin" : {"$geometry" : poi['coordinates']}}}


tweets_iterator = valencia.find()


for poi in pois.find():
    query = {'coordinates': {"$geoWithin" : {"$geometry" : poi['coordinates']}}}
    tweets = valencia.find(query)
    if tweets.count() > 0:
        print("Tweets inside {}: {} ".format(poi['name'], tweets.count()))
        for tweet in tweets:
            print("@{} twitted: {}".format(tweet['user']['id'], tweet['text']))
        print()

print(tweets_iterator.count())