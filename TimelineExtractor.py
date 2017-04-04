import tweepy
import operator
from pymongo import MongoClient
from pymongo import errors
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


    '''
    consumer_key = 'pqXRWujbYBVHXAAec9BJI3Lyq'
    consumer_secret = 'HZcAfoYS456x4R84MkoXn6bGisxCM1A6isn24m9ejsXQ2Eqxbw'
    access_token = '272959294-GLS2HDDlepTMVLVI0QqurdAa45SKRs9pGgwm15t1'
    access_secret = 'LvP4dCBcNG2FX2gpYy9sdapRYZ1efkb9eXkfdtitCqM9s'
    '''

    GEOBOX_VALENCIA = [-0.4315, 39.4196, -0.2857, 39.5045]


    def __init__(self):
        auth = tweepy.OAuthHandler(self.consumer_key, self.consumer_secret)
        auth.set_access_token(self.access_token, self.access_secret)

        self.api = tweepy.API(auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)
        self.provinces10 = gpd.read_file('/home/dieaigar/geodata/provinces10/ne_10m_admin_1_states_provinces.dbf')

        client = MongoClient('localhost', 27017)
        db = client['twitter_db']
        self.users_col = db['users']
        self.sequences_col = db['sequences']
        self.tweets_col = db['tourist_tweets']

    def store_local(self, user, country, last_review):
        my_user = user
        my_user['country'] = country
        my_user['last_review'] = last_review.timestamp()
        my_user['touristLocal'] = 'local'


        # Storing
        try:
            self.users_col.insert(my_user)

        except errors.DuplicateKeyError as e:
            print('Duplicate key, ', str(e))
            pass

    def store_tourist(self, user, country, sequences, tweets, last_review):
        my_user = user
        my_user['country'] = country
        my_user['last_review'] = last_review.timestamp()
        my_user['touristLocal'] = 'undefined'

        try:
            # Storing
            self.users_col.insert(my_user)
            for seq in sequences:
                self.sequences_col.insert(seq)
            for tweet in tweets:
                self.tweets_col.insert(tweet)

        except errors.DuplicateKeyError as e:
            print('Duplicate key, ', str(e))
            pass

    def update_local(self, user_id):
        my_user = self.users_col.find({'id' : user_id}).next()
        my_user['touristLocal'] = 'local'
        now = datetime.datetime.now()
        my_user['last_review'] = now.timestamp()*1000

        try:
            self.users_col.update_one({'id' : user_id}, {'$set': my_user}, upsert=False)
            self.tweets_col.delete_many({'user.id' : user_id})
            self.sequences_col.delete_many({'user_id' : user_id})
        except errors.DuplicateKeyError as e:
            print('Duplicate key, ', str(e))
            pass

    def update_tourist(self, user_id, tourist, sequences, tweets):
        my_user = self.users_col.find({'id' : user_id}).next()
        if tourist:
            my_user['touristLocal'] = 'tourist'
        now = datetime.datetime.now()
        my_user['last_review'] = now.timestamp() * 1000

        try:
            self.users_col.update_one({'id' : user_id}, {'$set': my_user}, upsert=False)
            for tweet in tweets:
                self.tweets_col.insert(tweet)
            for sequence in sequences:
                self.sequences_col.update_one({'user_id': sequence['user_id'], 'serial' : sequence['serial']}, {'$set': sequence}, upsert=True)
        except errors.DuplicateKeyError as e:
            print('Duplicate key, ', str(e))
            pass

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
        countries = {}
        current_sequence = None
        sequences = []
        last_review = None


        count = 0
        last_time_in_valencia = None

        tourist = False
        local = False

        tweet = None


        # statuses = tweepy.Cursor(api.user_timeline, id=174608487).items()
        statuses = tweepy.Cursor(self.api.user_timeline, id=user_id, max_id=tweet_id).items()


        # We explore the timeline backwards from the most recent to the least
        # Find the last place visited
        for status in statuses:
            tweet = json.loads(json.dumps(status._json))
            if tweet['coordinates'] != None:
                s = shape(tweet['coordinates'])

                tweet_province = self.provinces10.loc[:, ['admin', 'name']]
                tweet_province['distance'] = self.provinces10.distance(s)
                tweet_province = tweet_province.sort_values(by='distance')

                lastProvince = tweet_province.at[tweet_province.index[0], 'name']
                lastCountry = tweet_province.at[tweet_province.index[0], 'admin']

                last_review = lastDate = prevDate = datetime.datetime.strptime(tweet['created_at'], '%a %b %d %H:%M:%S +0000 %Y')

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
                    countries[lastCountry] = countries.get(lastCountry,0) + days
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
                    days = (lastDate - prevDate).days + 1

                    if newProvince == 'Valencia ciudad':
                        current_sequence['first'] = prevDate.timestamp()
                        current_sequence['sequence'] += [tweet['id']]


                        if places.get(newProvince,0) + days >= 50:
                            local = True
                            break

                    else:
                        if last_time_in_valencia is not None \
                                and (last_time_in_valencia - prevDate).days > 50:
                            tourist = True
                            break

                    count += 1




        if count > 1:
            days = (lastDate - prevDate).days + 1
        else:
            days = 1


        if len(places) == 0:
            print("No geolocated tweets")
            return
        if "Valencia ciudad" not in places.keys():
            print("No tweets from Valencia")
            return




        print("@{} visited {}, {} from {} to {} ({} days)({} tweets)".format(tweet['user']['name'], lastProvince, lastCountry, prevDate, lastDate, days, count))
        places[lastProvince] = places.get(lastProvince, 0) + days
        countries[lastCountry] = countries.get(lastCountry, 0) + days

        print("Tweets in Valencia = {}".format(len(tweets_in_valencia)))
        print(places)
        print(countries)

        seq_serial = len(sequences)-1
        for seq in sequences:
            seq['serial'] = seq_serial
            seq_serial -= 1
            print(seq)

        sorted_countries = sorted(countries.items(), key=operator.itemgetter(1), reverse=True)

        if local:
            print("Local confirmed")
            self.store_local(tweet['user'], sorted_countries[0][0], last_review)

        elif tourist:
            print("Tourist confirmed")
            self.store_tourist(tweet['user'], sorted_countries[0][0], sequences, tweets_in_valencia, last_review)
        else:
            print("Undefined: Analyzing timeline...")
            # Compute the places where he's been the most
            sorted_places = sorted(places.items(), key=operator.itemgetter(1), reverse=True)
            print("sorted places: ", sorted_places)
            max = sorted_places[0][1]
            max_places = []
            for place in sorted_places:
                if place[1] == max:
                    max_places.append(place[0])
                else:
                    break

            if len(places) <= 1:
                print("Never went outside Valencia... Local")
                self.store_local(tweet['user'], sorted_countries[0][0], last_review)
            elif 'Valencia ciudad' in max_places:
                print("He's been in Valencia the longest... Local")
                self.store_local(tweet['user'], sorted_countries[0][0], last_review)
            elif len(places) > 1 and places['Valencia ciudad'] < 50:
                print("Potential tourist")
                self.store_tourist(tweet['user'], sorted_countries[0][0], sequences, tweets_in_valencia, last_review)


    def timeline_since(self, last_sequence, days_in_valencia):

        user_id = last_sequence['user_id']
        tweet_id = last_sequence['sequence'][-1]
        serial = last_sequence['serial']

        tweets_in_valencia = []
        places = {}
        places['Valencia ciudad'] = days_in_valencia
        countries = {}
        current_sequence = None
        sequences = []


        count = 0
        last_time_in_valencia = datetime.datetime.fromtimestamp(last_sequence['last'])

        tourist = False
        local = False

        tweet = None



        statuses_iter = tweepy.Cursor(self.api.user_timeline, id=user_id, since_id=tweet_id).items()
        statuses_list = []

        for status in statuses_iter:
            statuses_list.append(status)

        statuses_list.reverse()

        statuses = iter(statuses_list)


        # We explore the timeline backwards from the most recent to the least
        # Find the last place visited
        for status in statuses:
            tweet = json.loads(json.dumps(status._json))
            if tweet['coordinates'] != None:
                s = shape(tweet['coordinates'])

                tweet_province = self.provinces10.loc[:, ['admin', 'name']]
                tweet_province['distance'] = self.provinces10.distance(s)
                tweet_province = tweet_province.sort_values(by='distance')

                newProvince = prevProvince = tweet_province.at[tweet_province.index[0], 'name']
                newCountry = prevCountry = tweet_province.at[tweet_province.index[0], 'admin']

                firstDate = newDate = datetime.datetime.strptime(tweet['created_at'], '%a %b %d %H:%M:%S +0000 %Y')

                count = 1

                if prevProvince == 'Valencia' and self.check_from_Valencia(tweet):
                    newProvince = prevProvince = 'Valencia ciudad'


                someDate = datetime.datetime.strptime(tweet['created_at'], '%a %b %d %H:%M:%S +0000 %Y')
                print("@{} visited {}, {} on {}".format(tweet['id'], prevProvince, prevCountry, someDate))

                if prevProvince == 'Valencia ciudad':
                    firstDate = datetime.datetime.fromtimestamp(last_sequence['first'])
                    tweets_in_valencia += [tweet]
                    current_sequence = last_sequence
                    current_sequence['last'] = newDate.timestamp()
                    current_sequence['sequence'] += [tweet['id']]

                    #Correct places['Valencia_ciudad'] counter
                    days = (last_time_in_valencia - datetime.datetime.fromtimestamp(current_sequence['first'])).days + 1
                    places['Valencia ciudad'] -= days

                elif (newDate - last_time_in_valencia).days + 1 > 50:
                    print("He left Valencia more than 50 days ago... Tourist confirmed")
                    tourist = True
                    self.update_tourist(user_id, tourist, sequences, tweets_in_valencia)
                    return

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

                someDate = datetime.datetime.strptime(tweet['created_at'], '%a %b %d %H:%M:%S +0000 %Y')

                if newProvince == 'Valencia' and self.check_from_Valencia(tweet):
                    tweets_in_valencia += [tweet]
                    newProvince = 'Valencia ciudad'
                elif (someDate - last_time_in_valencia).days + 1 > 50:
                    print("He left Valencia more than 50 days ago... Tourist confirmed")
                    tourist = True
                    break


                print("@{} visited {}, {} on {}".format(tweet['id'], newProvince, newCountry, someDate))


                if prevProvince != newProvince:

                    if count > 1:
                        days = (newDate-firstDate).days + 1
                    else:
                        days = 1

                    print("@{} visited {}, {} from {} to {} ({} days)({} tweets)".format(tweet['user']['name'], prevProvince, prevCountry, firstDate, newDate, days, count))

                    places[prevProvince] = places.get(prevProvince,0) + days
                    countries[prevCountry] = countries.get(prevCountry,0) + days

                    prevDate = newDate
                    newDate = firstDate = datetime.datetime.strptime(tweet['created_at'], '%a %b %d %H:%M:%S +0000 %Y')

                    if newProvince == 'Valencia ciudad':
                        serial += 1
                        current_sequence = {}
                        current_sequence['user_id'] = user_id
                        current_sequence['serial'] = serial
                        current_sequence['first'] = firstDate.timestamp()
                        current_sequence['last'] = newDate.timestamp()
                        current_sequence['sequence'] = [tweet['id']]
                    elif prevProvince == 'Valencia ciudad':
                        sequences += [current_sequence]
                        last_time_in_valencia = prevDate
                        current_sequence = None


                    prevProvince = newProvince
                    prevCountry = newCountry

                    count = 1

                else:

                    newDate = datetime.datetime.strptime(tweet['created_at'], '%a %b %d %H:%M:%S +0000 %Y')
                    days = (newDate - firstDate).days + 1

                    if newProvince == 'Valencia ciudad':
                        current_sequence['last'] = newDate.timestamp()
                        current_sequence['sequence'] += [tweet['id']]



                        if places.get(newProvince,0) + days >= 50:
                            local = True
                            break

                    count += 1

        now = datetime.datetime.now()

        if count == 0:
            print("No new geotagged tweets...")
            self.update_tourist(user_id, False, [], [])
            return


        if count > 1:
            days = (newDate - firstDate).days + 1
        else:
            days = 1

        if current_sequence is not None:
            sequences.append(current_sequence)


        print("@{} visited {}, {} from {} to {} ({} days)({} tweets)".format(tweet['user']['name'], newProvince, newCountry, firstDate, newDate, days, count))
        places[newProvince] = places.get(newProvince, 0) + days
        countries[newCountry] = countries.get(newCountry, 0) + days

        print("Tweets in Valencia = {}".format(len(tweets_in_valencia)))
        print(places)
        print(countries)

        if local:
            print("Local confirmed")
            print(last_sequence)
            for sequence in sequences:
                print(sequence)
            self.update_local(user_id)

        else:
            if tourist:
                print("Tourist confirmed")
            else:
                print("Undefined, keep him under observation")

            print(last_sequence)
            for sequence in sequences:
                print(sequence)
            self.update_tourist(user_id, tourist, sequences, tweets_in_valencia)

    def deb_timeline(self, user_id):

        statuses = tweepy.Cursor(self.api.user_timeline, id=user_id).items()
        count = 0
        for status in statuses:
            if count > 50:
                break
            tweet = json.loads(json.dumps(status._json))
            print(tweet['text'])
            count += 1


