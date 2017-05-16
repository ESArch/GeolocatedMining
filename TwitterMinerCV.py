import json

from pymongo import MongoClient
from pymongo import errors
from shapely.geometry import shape, mapping, MultiPolygon, Polygon
from tweepy import OAuthHandler
from tweepy import Stream
from tweepy import StreamListener
import geopandas as gpd
import fiona
import matplotlib.pyplot as plt



# User credentials to access the Twitter API
consumer_key = 'Dg4wbricgJUgCg7qgkun8yBWZ'
consumer_secret = 'ktUfXtYV2QvXzdtx27nebWY8Y3wE9HvALVu8MaCJITMafK22vm'
access_token = '4251003400-hM53ycuOYzsUhv0jTv4Di2Tl8Z7HjuHZeAFHIMX'
access_secret = 'EXpy5d3jJfTEfOlAqjy34TINRR23KpS7Zc4VeVS3tKaNT'

auth = OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_secret)

provinces = gpd.read_file('/home/dieaigar/geodata/provinces10/ne_10m_admin_1_states_provinces.dbf')
provinces = provinces.loc[:, ['admin', 'name', 'geometry']]
spain_provinces = provinces.loc[lambda df: df['admin'] == "Spain"]

# print(spain_provinces)




# Basic listener
class StdOutListener(StreamListener):

    def on_data(self, data):
        #print(data)
        try:
            # Connection


            # Parsing
            tweet = json.loads(data)

            # Storing
            if(tweet['coordinates']!=None):

                # print(tweet['text'])
                s = shape(tweet['coordinates'])

                tweet_provinces = gpd.GeoDataFrame(spain_provinces)
                tweet_provinces['distance'] = spain_provinces.distance(s)
                tweet_provinces = tweet_provinces.sort_values(by='distance')

                my_province = tweet_provinces.at[tweet_provinces.index[0], 'name']



                # print(my_province)

                if my_province in ['Valencia', 'Alicante', 'Castellón']:
                    # print("A guardar!")

                    client = MongoClient('localhost', 27017)
                    db = client['twitter_db']
                    collection = db['cv_collection']
                    collection.insert(tweet)

            return True

        except BaseException as e:
            print('failed ondata,',str(e))
            pass

        except errors.DuplicateKeyError as e:
            print('Duplicate key, ', str(e))
            pass

        exit()

    def on_error(self, status_code):
        print(status_code)


    def start_stream(self):
        while True:
            try:
                stream = Stream(auth, l)
                stream.filter(locations=GEOBOX_VALENCIACV)
            except:
                continue



if __name__ == '__main__':

    l = StdOutListener()
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_secret)

    GEOBOX_VALENCIACV = [-1.5288, 37.8439, 0.6889, 40.7877]

    while True:
        try:
            stream = Stream(auth, l)
            stream.filter(locations=GEOBOX_VALENCIACV)
        except:
            continue

