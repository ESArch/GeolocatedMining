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

class Timeline:

    consumer_key = 'Dg4wbricgJUgCg7qgkun8yBWZ'
    consumer_secret = 'ktUfXtYV2QvXzdtx27nebWY8Y3wE9HvALVu8MaCJITMafK22vm'
    access_token = '4251003400-hM53ycuOYzsUhv0jTv4Di2Tl8Z7HjuHZeAFHIMX'
    access_secret = 'EXpy5d3jJfTEfOlAqjy34TINRR23KpS7Zc4VeVS3tKaNT'

    GEOBOX_VALENCIA = [-0.4315, 39.4196, -0.2857, 39.5045]



    def __init__(self):
        auth = tweepy.OAuthHandler(self.consumer_key, self.consumer_secret)
        auth.set_access_token(self.access_token, self.access_secret)

        self.api = tweepy.API(auth,wait_on_rate_limit=True)
        self.provinces10 = gpd.read_file('/home/dieaigar/geodata/provinces10/ne_10m_admin_1_states_provinces.dbf')

    def check_from_Valencia(self, tweet):


        lon = tweet['coordinates']['coordinates'][0]
        lat = tweet['coordinates']['coordinates'][1]

        if lon > self.GEOBOX_VALENCIA[0] and lon < self.GEOBOX_VALENCIA[2] \
                and lat > self.GEOBOX_VALENCIA[1] and lat < self.GEOBOX_VALENCIA[3]:
            return True

        return False

    def timeline_until(self, user_id, tweet_id):

        tweets_in_valencia = []
        places = {}
        current_sequence = {}
        sequences = []


        count = 0
        last_time_in_valencia = None

        tourist = False
        local = False



        # statuses = tweepy.Cursor(api.user_timeline, id=174608487).items()
        statuses = tweepy.Cursor(self.api.user_timeline, id=user_id, max_id=tweet_id).items()


        # We explore the timeline backwards from the most recent to the least
        # Find de last place visited
        for status in statuses:
            tweet = json.loads(json.dumps(status._json))
            if tweet['coordinates'] != None:
                s = shape(tweet['coordinates'])

                tweet_province = self.provinces10.loc[:, ['admin', 'name']]
                tweet_province['distance'] = self.provinces10.distance(s)
                tweet_province = tweet_province.sort_values(by='distance')

                lastProvince = tweet_province.at[tweet_province.index[0], 'name']
                lastCountry = tweet_province.at[tweet_province.index[0], 'admin']

                lastDate = prevDate = datetime.datetime.strptime(tweet['created_at'], '%a %b %d %H:%M:%S +0000 %Y')

                count = 1

                if lastProvince == 'Valencia' and self.check_from_Valencia(tweet):
                    tweets_in_valencia += [tweet]
                    lastProvince = 'Valencia ciudad'
                    current_sequence = {}
                    current_sequence['user_id'] = user_id
                    current_sequence['serial'] = len(sequences)
                    current_sequence['first'] = lastDate.timestamp()
                    current_sequence['last'] = lastDate.timestamp()
                    current_sequence['sequence'] = [tweet['id']]


                break

        for status in statuses:
            tweet = json.loads(json.dumps(status._json))
            if tweet['coordinates'] != None:

                s = shape(tweet['coordinates'])

                # Using distance

                tweet_province = self.provinces10.loc[:, ['admin', 'name']]
                tweet_province['distance'] = self.provinces10.distance(s)
                tweet_province = tweet_province.sort_values(by='distance')

                newProvince = tweet_province.at[tweet_province.index[0], 'name']
                newCountry = tweet_province.at[tweet_province.index[0], 'admin']

                if newProvince == 'Valencia' and self.check_from_Valencia(tweet):
                    tweets_in_valencia += [tweet]
                    newProvince = 'Valencia ciudad'



                if lastProvince != newProvince:

                    if count > 1:
                        days = (lastDate-prevDate).days + 1
                    else:
                        days = 1

                    print("@{} visited {}, {} from {} to {} ({} days)({} tweets)".format(tweet['user']['name'], lastProvince, lastCountry, prevDate, lastDate, days, count))

                    places[lastProvince] = places.get(lastProvince,0) + days
                    prevDate = lastDate = datetime.datetime.strptime(tweet['created_at'], '%a %b %d %H:%M:%S +0000 %Y')

                    if newProvince == 'Valencia ciudad':
                        current_sequence = {}
                        current_sequence['user_id'] = user_id
                        current_sequence['serial'] = len(sequences)
                        current_sequence['first'] = lastDate.timestamp()
                        current_sequence['last'] = lastDate.timestamp()
                        current_sequence['sequence'] = [tweet['id']]
                    elif lastProvince == 'Valencia ciudad':
                        sequences += [current_sequence]
                        last_time_in_valencia = lastDate


                    lastProvince = newProvince
                    lastCountry = newCountry

                    count = 1

                else:

                    prevDate = datetime.datetime.strptime(tweet['created_at'], '%a %b %d %H:%M:%S +0000 %Y')

                    if newProvince == 'Valencia ciudad':
                        current_sequence['first'] = prevDate.timestamp()
                        current_sequence['sequence'] += [tweet['id']]


                        if places.get(newProvince,0) + count >= 50:
                            local = True
                            break;


                    count += 1

                    if last_time_in_valencia is not None \
                            and (last_time_in_valencia - prevDate).days > 50:
                        tourist = True
                        break


        if count > 1:
            days = (lastDate - prevDate).days + 1
        else:
            days = 1


        print("@{} visited {}, {} from {} to {} ({} days)({} tweets)".format(tweet['user']['name'], lastProvince, lastCountry, prevDate, lastDate, days, count))
        places[lastProvince] = places.get(lastProvince, 0) + days

        print("Tweets in Valencia = {}".format(len(tweets_in_valencia)))
        print(places)
        for seq in sequences:
            print(seq)

        if local:
            print("Local confirmed")
        elif tourist:
            print("Tourist confirmed")
        else:
            print("Undefined: Analyzing tweets...")
            if len(places) > 1 and places['Valencia ciudad'] < 50:
                print("Potential tourist")



