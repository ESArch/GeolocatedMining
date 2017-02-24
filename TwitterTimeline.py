import tweepy
import geocoder
import datetime
import pymongo
from bson import SON
from instagram import client
import WebScraping
import geopandas as gpd
from shapely.geometry import MultiPolygon
from shapely.geometry import mapping, shape



import json


GEOBOX_VALENCIA = [-0.4315, 39.4196, -0.2857, 39.5045]
limit = 100

# User credentials to access the Twitter API
consumer_key = 'Dg4wbricgJUgCg7qgkun8yBWZ'
consumer_secret = 'ktUfXtYV2QvXzdtx27nebWY8Y3wE9HvALVu8MaCJITMafK22vm'
access_token = '4251003400-hM53ycuOYzsUhv0jTv4Di2Tl8Z7HjuHZeAFHIMX'
access_secret = 'EXpy5d3jJfTEfOlAqjy34TINRR23KpS7Zc4VeVS3tKaNT'


auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_secret)


api = tweepy.API(auth)

def tweets_in_valencia():

    notValencia = 0


    for status in tweepy.Cursor(api.user_timeline, id=174608487).items():
        tweet = json.loads(json.dumps(status._json))
        if tweet['coordinates'] != None:
            lon = tweet['coordinates']['coordinates'][0]
            lat = tweet['coordinates']['coordinates'][1]

            if lon > GEOBOX_VALENCIA[0] and lon < GEOBOX_VALENCIA[2] \
                and lat > GEOBOX_VALENCIA[1] and lat < GEOBOX_VALENCIA[3]:

                notValencia = 0
                print(tweet['text'])

            else:

                notValencia += 1

        else:
            notValencia += 1

        if notValencia > limit:
            break

def actions_in_valencia():

    client = pymongo.MongoClient('localhost', 27017)
    db = client['twitter_db']
    pois = db['poi']



    notValencia = 0
    firstFound = False

    #174608487

    for status in tweepy.Cursor(api.user_timeline, id=174608487).items():
        tweet = json.loads(json.dumps(status._json))
        if tweet['coordinates'] != None:
            lon = tweet['coordinates']['coordinates'][0]
            lat = tweet['coordinates']['coordinates'][1]

            if lon > GEOBOX_VALENCIA[0] and lon < GEOBOX_VALENCIA[2] \
                and lat > GEOBOX_VALENCIA[1] and lat < GEOBOX_VALENCIA[3]:
                firstFound = True

                date = datetime.datetime.strptime(tweet['created_at'], '%a %b %d %H:%M:%S +0000 %Y')
                poisNear = pois.find({'coordinates' : SON([('$near' , tweet['coordinates']) , ('$maxDistance' , 15 ) ]) })


                poi = "UNKNOWN"
                if poisNear.count() > 0:
                    poi = poisNear.next()['name']

                #trueDate = WebScraping.getMediaDate(tweet)
                print("@{} twitted from {} on {}".format(tweet['user']['name'], poi, date))
                mediaURL = tweet['entities']['urls'][0]['expanded_url']
                if 'instagram' in mediaURL:
                    print(WebScraping.getMediaDate(mediaURL))


                notValencia = 0

            else:
                notValencia += 1

        else:
            notValencia += 1

        if notValencia > limit and firstFound:
            break


def places_in_timeline():
    #statuses = tweepy.Cursor(api.user_timeline, id=174608487).items()
    statuses = tweepy.Cursor(api.user_timeline, id=174608487).items()
    for status in statuses:
        tweet = json.loads(json.dumps(status._json))
        if tweet['coordinates'] != None:
            #g = geocoder.google([tweet['coordinates']['coordinates'][1], tweet['coordinates']['coordinates'][0]], method='reverse')
            g = geocoder.google([tweet['coordinates']['coordinates'][1], tweet['coordinates']['coordinates'][0]], method='reverse')
            newCity = g.city
            newCountry = g.country_long
            newDate = datetime.datetime.strptime(tweet['created_at'], '%a %b %d %H:%M:%S +0000 %Y')

            mediaURL = tweet['entities']['urls'][0]['expanded_url']
            if 'instagram' in mediaURL:
                trueDate = WebScraping.getMediaDate(mediaURL)

            #print("@{} visited {}, {} on {}".format(tweet['user']['name'], newCity, newCountry, newDate))
            break

    for status in statuses:
        tweet = json.loads(json.dumps(status._json))
        if tweet['coordinates'] != None:
            #g = geocoder.google([tweet['coordinates']['coordinates'][1], tweet['coordinates']['coordinates'][0]], method='reverse')
            g = geocoder.google([tweet['coordinates']['coordinates'][1], tweet['coordinates']['coordinates'][0]], method='reverse')
            if newCity != g.city:
                print("@{} visited {}, {} on {}".format(tweet['user']['name'], newCity, newCountry, newDate))
                print(trueDate)

                newCity = g.city
                newCountry = g.country_long

            newDate = datetime.datetime.strptime(tweet['created_at'], '%a %b %d %H:%M:%S +0000 %Y')
            mediaURL = tweet['entities']['urls'][0]['expanded_url']
            if 'instagram' in mediaURL:
                trueDate = WebScraping.getMediaDate(mediaURL)

    print("@{} visited {}, {} on {}".format(tweet['user']['name'], newCity, newCountry, newDate))

'''
def visited_places_with_local_geocoder():
    provinces10 = gpd.read_file('/home/dieaigar/geodata/provinces10/ne_10m_admin_1_states_provinces.dbf')

    #statuses = tweepy.Cursor(api.user_timeline, id=174608487).items()
    statuses = tweepy.Cursor(api.user_timeline, id=174608487).items()


    # We explore the timeline backwards from the most recent to the least
    # Find de last place visited
    for status in statuses:
        tweet = json.loads(json.dumps(status._json))
        if tweet['coordinates'] != None:
            s = shape(tweet['coordinates'])

            tweet_province = provinces10.loc[:, ['admin', 'name']]
            tweet_province['distance'] = provinces10.distance(s)
            tweet_province = tweet_province.sort_values(by='distance')


            lastProvince = tweet_province.at[tweet_province.index[0], 'name']
            lastCountry = tweet_province.at[tweet_province.index[0], 'admin']


            lastDate = prevDate = datetime.datetime.strptime(tweet['created_at'], '%a %b %d %H:%M:%S +0000 %Y')

            break



    for status in statuses:
        tweet = json.loads(json.dumps(status._json))
        if tweet['coordinates'] != None:

            s = shape(tweet['coordinates'])

            # Using distance

            tweet_province = provinces10.loc[:, ['admin', 'name']]
            tweet_province['distance'] = provinces10.distance(s)
            tweet_province = tweet_province.sort_values(by='distance')


            newProvince = tweet_province.at[tweet_province.index[0], 'name']
            newCountry = tweet_province.at[tweet_province.index[0], 'admin']


            if lastProvince != newProvince:
                print("@{} visited {}, {} from {} to {}".format(tweet['user']['name'], lastProvince, lastCountry, prevDate, lastDate))

                lastProvince = newProvince
                lastCountry = newCountry
                prevDate = lastDate = datetime.datetime.strptime(tweet['created_at'], '%a %b %d %H:%M:%S +0000 %Y')

            else:
                prevDate = datetime.datetime.strptime(tweet['created_at'], '%a %b %d %H:%M:%S +0000 %Y')

    print("@{} visited {}, {} from {} to {}".format(tweet['user']['name'], lastProvince, lastCountry, prevDate, lastDate))
'''

def visited_places_with_local_geocoder(user_id, tweet_id):
    provinces10 = gpd.read_file('/home/dieaigar/geodata/provinces10/ne_10m_admin_1_states_provinces.dbf')

    #statuses = tweepy.Cursor(api.user_timeline, id=174608487).items()
    statuses = tweepy.Cursor(api.user_timeline, id=user_id, max_id=tweet_id).items()


    # We explore the timeline backwards from the most recent to the least
    # Find de last place visited
    for status in statuses:
        tweet = json.loads(json.dumps(status._json))
        if tweet['coordinates'] != None:
            s = shape(tweet['coordinates'])

            tweet_province = provinces10.loc[:, ['admin', 'name']]
            tweet_province['distance'] = provinces10.distance(s)
            tweet_province = tweet_province.sort_values(by='distance')


            lastProvince = tweet_province.at[tweet_province.index[0], 'name']
            lastCountry = tweet_province.at[tweet_province.index[0], 'admin']


            lastDate = prevDate = datetime.datetime.strptime(tweet['created_at'], '%a %b %d %H:%M:%S +0000 %Y')

            break



    for status in statuses:
        tweet = json.loads(json.dumps(status._json))
        if tweet['coordinates'] != None:

            s = shape(tweet['coordinates'])

            # Using distance

            tweet_province = provinces10.loc[:, ['admin', 'name']]
            tweet_province['distance'] = provinces10.distance(s)
            tweet_province = tweet_province.sort_values(by='distance')


            newProvince = tweet_province.at[tweet_province.index[0], 'name']
            newCountry = tweet_province.at[tweet_province.index[0], 'admin']


            if lastProvince != newProvince:
                print("@{} visited {}, {} from {} to {}".format(tweet['user']['name'], lastProvince, lastCountry, prevDate, lastDate))

                lastProvince = newProvince
                lastCountry = newCountry
                prevDate = lastDate = datetime.datetime.strptime(tweet['created_at'], '%a %b %d %H:%M:%S +0000 %Y')

            else:
                prevDate = datetime.datetime.strptime(tweet['created_at'], '%a %b %d %H:%M:%S +0000 %Y')

    print("@{} visited {}, {} from {} to {}".format(tweet['user']['name'], lastProvince, lastCountry, prevDate, lastDate))




def timeline_with_local_geocoder():
    provinces10 = gpd.read_file('/home/dieaigar/geodata/provinces10/ne_10m_admin_1_states_provinces.dbf')

    #statuses = tweepy.Cursor(api.user_timeline, id=174608487).items()
    statuses = tweepy.Cursor(api.user_timeline, id=174608487).items()


    # We explore the timeline backwards from the most recent to the least
    # Find de last place visited
    for status in statuses:
        tweet = json.loads(json.dumps(status._json))
        if tweet['coordinates'] != None:
            s = shape(tweet['coordinates'])

            # Using intersect
            '''
            tweet_province = provinces10[provinces10.intersects(s)].loc[:, ['admin', 'name', 'woe_name']]

            if len(tweet_province.index) > 0:
                province = tweet_province.at[tweet_province.index[0], 'name']
                country = tweet_province.at[tweet_province.index[0], 'admin']

            else:
                province = country = "Unknown"
            '''

            # Using distance

            tweet_province = provinces10.loc[:, ['admin', 'name']]
            tweet_province['distance'] = provinces10.distance(s)
            tweet_province = tweet_province.sort_values(by='distance')

            province = tweet_province.at[tweet_province.index[0], 'name']
            country = tweet_province.at[tweet_province.index[0], 'admin']


            date = datetime.datetime.strptime(tweet['created_at'], '%a %b %d %H:%M:%S +0000 %Y')

            print("{} was in {}, {} on {}".format(tweet['user']['name'], province, country, date))


#visited_places_with_local_geocoder()


#actions_in_valencia()

#places_in_timeline()

#timeline_with_local_geocoder()


def ins():
    access_token = "4228636223.5819937.bcdf492d5be5441c92d3a1ea26ad25c7"
    client_secret = "ee7e762a3723477c8390173f94a700cd"
    api = client.InstagramAPI(access_token=access_token, client_secret=client_secret)
    medias = api.media("BNXlsi5jtnD")
    for media in medias:
       print(media)







