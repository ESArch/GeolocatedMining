from pymongo import MongoClient
import pymongo
import datetime
from bson import SON
import re

client = MongoClient('localhost', 27017)
db = client['twitter_db']
tcol = db['tourist_tweets']
pcol = db['poi']
ucol = db['users']
scol = db['sequences']


def build_itemsets(distance, undefined=True, only_tourists=True):
    hash_table = dict()
    count = 0

    f = open('input/is_user.txt', mode='w')
    g = open('translation_tables/is_user_tt', mode='w')



    q1 = {'touristLocal': 'tourist'}
    q2 = {'$or': [{'touristLocal': 'tourist'}, {'touristLocal': 'undefined'}]}

    if only_tourists:
        query = q1
    else:
        query = q2

    for tourist in ucol.find(query):
        tweets = []
        item_set = set()
        for sequence in scol.find({'user_id': tourist['id']}).sort('serial', pymongo.ASCENDING):
            for id in sequence['sequence']:
                for tweet in tcol.find({'id': id}):
                    tweets.append(tweet)

                    lon = tweet['coordinates']['coordinates'][0]
                    lat = tweet['coordinates']['coordinates'][1]
                    queryNear = {'coordinates': SON(
                        [('$near', {"type": "Point", "coordinates": [lon, lat]}), ('$maxDistance', distance)])}



                    close_pois = pcol.find(queryNear)

                    closest_poi = 'UNDEFINED'

                    if close_pois.count() > 0:
                        closest_poi = close_pois.next()['name']
                    else:
                        if not undefined:
                            continue

                    '''
                    print(lon, lat)
                    print(closest_poi)

                    for poi in pcol.find():
                        queryWithin = {'id': tweet['id'],'coordinates': {"$geoWithin" : {"$geometry" : poi['coordinates']}}}
                        for tweet in tcol.find(queryWithin):
                            print("tweeted from inside of {}".format(poi['name']))
                    '''

                    code = hash_table.get(closest_poi, 0)
                    if code == 0:
                        count += 1
                        hash_table[closest_poi] = count
                        code = count


                    item_set.add(code)

        if len(item_set) > 0:
            f.write(" ".join(list(map(str, sorted(item_set)))))
            f.write("\n")

    aux = []
    for key,value in hash_table.items():
        aux += [(key, value)]

    sorted_aux = sorted(aux, key=lambda tup: tup[1])

    for item in sorted_aux:
        g.write(str(item[1]) + "\t" + item[0] + "\n")


def build_itemsets_by_day(distance, undefined=True, only_tourists=True):
    hash_table = dict()
    count = 0

    f = open('DatasetBuilding/input/is_userdays.txt', mode='w')
    g = open('DatasetBuilding/translation_tables/is_userdays_tt', mode='w')

    q1 = {'touristLocal': 'tourist'}
    q2 = {'$or': [{'touristLocal': 'tourist'}, {'touristLocal': 'undefined'}]}

    if only_tourists:
        query = q1
    else:
        query = q2

    for tourist in ucol.find(query):
        tweets = []
        item_set = set()
        prevDate = None
        for sequence in scol.find({'user_id': tourist['id']}).sort('serial', pymongo.ASCENDING):
            for id in sequence['sequence']:
                for tweet in tcol.find({'id': id}):

                    currentDate = datetime.datetime.strptime(tweet['created_at'], '%a %b %d %H:%M:%S +0000 %Y')

                    if prevDate is None:
                        prevDate = currentDate

                    if (currentDate.date() - prevDate.date()).days > 0:
                        f.write(" ".join(list(map(str, sorted(item_set)))))
                        f.write("\n")
                        item_set = set()

                    #print(currentDate)

                    tweets.append(tweet)

                    lon = tweet['coordinates']['coordinates'][0]
                    lat = tweet['coordinates']['coordinates'][1]
                    queryNear = {'coordinates': SON(
                        [('$near', {"type": "Point", "coordinates": [lon, lat]}), ('$maxDistance', distance)])}

                    close_pois = pcol.find(queryNear).limit(1)

                    closest_poi = 'UNDEFINED'

                    if close_pois.count() > 0:
                        closest_poi = close_pois.next()['name']
                    else:
                        if not undefined:
                            continue

                    code = hash_table.get(closest_poi, 0)
                    if code == 0:
                        count += 1
                        hash_table[closest_poi] = count
                        code = count

                    item_set.add(code)

        if len(item_set) > 0:
            f.write(" ".join(list(map(str, sorted(item_set)))))
            f.write("\n")

    aux = []
    for key, value in hash_table.items():
        aux += [(key, value)]

    sorted_aux = sorted(aux, key=lambda tup: tup[1])

    for item in sorted_aux:
        g.write(str(item[1]) + "\t" + item[0] + "\n")

def build_sequences(distance, undefined=True, only_tourists=True):
    hash_table = dict()
    count = 0

    f = open('DatasetBuilding/input/seq_user.txt', mode='w')
    g = open('DatasetBuilding/translation_tables/seq_user_tt', mode='w')

    q1 = {'touristLocal': 'tourist'}
    q2 = {'$or': [{'touristLocal': 'tourist'}, {'touristLocal': 'undefined'}]}

    if only_tourists:
        query = q1
    else:
        query = q2

    for tourist in ucol.find(query):
        tweets = []
        tourist_sequence = []
        for sequence in scol.find({'user_id': tourist['id']}).sort('serial', pymongo.ASCENDING):
            for id in sequence['sequence']:
                for tweet in tcol.find({'id': id}):
                    tweets.append(tweet)

                    lon = tweet['coordinates']['coordinates'][0]
                    lat = tweet['coordinates']['coordinates'][1]
                    queryNear = {'coordinates': SON(
                        [('$near', {"type": "Point", "coordinates": [lon, lat]}), ('$maxDistance', distance)])}

                    close_pois = pcol.find(queryNear).limit(1)

                    closest_poi = 'UNDEFINED'

                    if close_pois.count() > 0:
                        closest_poi = close_pois.next()['name']
                    else:
                        if not undefined:
                            continue

                    code = hash_table.get(closest_poi, 0)
                    if code == 0:
                        count += 1
                        hash_table[closest_poi] = count
                        code = count

                    tourist_sequence.append(str(code))

        if len(tourist_sequence) > 0:
            f.write(" -1 ".join(list(tourist_sequence)))
            f.write(" -1 -2\n")

    aux = []
    for key, value in hash_table.items():
        aux += [(key, value)]

    sorted_aux = sorted(aux, key=lambda tup: tup[1])

    for item in sorted_aux:
        g.write(str(item[1]) + "\t" + item[0] + "\n")

def build_sequences_by_day(distance, undefined=True, only_tourists=True):
    hash_table = dict()
    count = 0

    f = open('DatasetBuilding/input/seq_userdays.txt', mode='w')
    g = open('DatasetBuilding/translation_tables/seq_userdays_tt', mode='w')

    q1 = {'touristLocal': 'tourist'}
    q2 = {'$or': [{'touristLocal': 'tourist'}, {'touristLocal': 'undefined'}]}

    if only_tourists:
        query = q1
    else:
        query = q2

    for tourist in ucol.find(query):
        tweets = []
        item_set = set()
        prevDate = None
        seq_str = ""
        for sequence in scol.find({'user_id': tourist['id']}).sort('serial', pymongo.ASCENDING):
            for id in sequence['sequence']:
                for tweet in tcol.find({'id': id}):

                    currentDate = datetime.datetime.strptime(tweet['created_at'], '%a %b %d %H:%M:%S +0000 %Y')

                    if prevDate is None:
                        prevDate = currentDate

                    if (currentDate.date() - prevDate.date()).days > 0:
                        #f.write(" ".join(tourist_sequence))
                        #f.write(" -1 ")
                        if len(item_set) > 0:
                            seq_str += " ".join(list(map(str, sorted(item_set)))) + " -1 "
                        item_set = set()

                    # print(currentDate)

                    tweets.append(tweet)

                    lon = tweet['coordinates']['coordinates'][0]
                    lat = tweet['coordinates']['coordinates'][1]
                    queryNear = {'coordinates': SON(
                        [('$near', {"type": "Point", "coordinates": [lon, lat]}), ('$maxDistance', distance)])}

                    close_pois = pcol.find(queryNear).limit(1)

                    closest_poi = 'UNDEFINED'

                    if close_pois.count() > 0:
                        closest_poi = close_pois.next()['name']
                    else:
                        if not undefined:
                            continue

                    code = hash_table.get(closest_poi, 0)
                    if code == 0:
                        count += 1
                        hash_table[closest_poi] = count
                        code = count

                    item_set.add(code)

        if len(item_set) > 0:
            seq_str += " ".join(list(map(str, sorted(item_set)))) + " -1 "
        if seq_str != "":
            seq_str += "-2\n"
            f.write(seq_str)
        #f.write(" ".join(tourist_sequence))
        #f.write(" -1 -2\n")

    aux = []
    for key, value in hash_table.items():
        aux += [(key, value)]

    sorted_aux = sorted(aux, key=lambda tup: tup[1])

    for item in sorted_aux:
        g.write(str(item[1]) + "\t" + item[0] + "\n")


def decode(output, decoded, translation_table):
    places = []

    f = open("DatasetBuilding/translation_tables/" + translation_table, 'r')
    lines = f.readlines()
    for line in lines:
        places.append(line.split("\t")[1].strip())

    f = open("DatasetBuilding/output/" + output, 'r')
    lines = f.readlines()

    g = open("DatasetBuilding/decoded/" + decoded, 'w')
    for line in lines:
        splitted_line = re.split(',| ', line)
        for i in range(len(splitted_line)):
            if "#SUP" not in splitted_line[i]:
                try:
                    splitted_line[i] = places[int(splitted_line[i]) - 1]
                except:
                    continue
            else:
                break
        g.write(" ".join(splitted_line))







build_itemsets(15, undefined=False, only_tourists=True)
# build_itemsets_by_day(25, undefined=False, only_tourists=True)
# build_sequences(25, undefined=False, only_tourists=True)
# build_sequences_by_day(25, undefined=False, only_tourists=True)



# decode("is_user.out", "is_user", "is_user_tt")
# decode("is_userdays.out", "is_userdays", "is_userdays_tt")
# decode("seq_user.out", "seq_user", "seq_user_tt")
# decode("seq_userdays.out", "seq_userdays", "seq_userdays_tt")
