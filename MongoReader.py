from pymongo import MongoClient
import pandas as pd
import matplotlib.pyplot as plt
import json

client = MongoClient('localhost', 27017)
db = client['twitter_db']
collection = db['valencia_collection']

tweets_iterator = collection.find()

for tweet in tweets_iterator:
    print(tweet['text'],'\n')


print(tweets_iterator.count())

