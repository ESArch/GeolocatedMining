from pymongo import MongoClient
import pandas as pd
import matplotlib.pyplot as plt
import json

client = MongoClient('localhost', 27017)
db = client['twitter_db']
collection = db['valencia_collection']

f = open('englishTweets.txt', 'w', encoding='utf8')

tweets_iterator = collection.find({'lang': 'en'})

for tweet in tweets_iterator:
    #print(tweet['text'],'\n')
    mystr = '. '.join(tweet['text'].splitlines())
    f.write(mystr+'\n')
    #print(mystr)

print(tweets_iterator.count())

