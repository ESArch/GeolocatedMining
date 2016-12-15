import time
from tweepy import OAuthHandler
from tweepy import Stream
from tweepy import StreamListener

from pymongo import MongoClient
import json

# User credentials to access the Twitter API
consumer_key = 'Dg4wbricgJUgCg7qgkun8yBWZ'
consumer_secret = 'ktUfXtYV2QvXzdtx27nebWY8Y3wE9HvALVu8MaCJITMafK22vm'
access_token = '4251003400-hM53ycuOYzsUhv0jTv4Di2Tl8Z7HjuHZeAFHIMX'
access_secret = 'EXpy5d3jJfTEfOlAqjy34TINRR23KpS7Zc4VeVS3tKaNT'

auth = OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_secret)

# Basic listener
class StdOutListener(StreamListener):

    def on_data(self, data):
        #print(data)
        try:
            # Connection
            client = MongoClient('localhost', 27017)
            db = client['twitter_db']
            collection = db['valencia_collection']

            # Parsing
            tweet = json.loads(data)

            # Storing
            if(tweet['coordinates']!=None):
                collection.insert(tweet)

            return True

        except BaseException as e:
            print('failed ondata,',str(e))
            pass

        exit()

    def on_error(self, status_code):
        print(status_code)



if __name__ == '__main__':

    l = StdOutListener()
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_secret)
    stream = Stream(auth, l)

    GEOBOX_VALENCIA = [-0.4315, 39.4196, -0.2857, 39.5045]

    stream.filter(locations = GEOBOX_VALENCIA)

