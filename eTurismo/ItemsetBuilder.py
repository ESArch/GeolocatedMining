from pymongo import MongoClient
import pymongo
import datetime
from bson import SON
import re
import psycopg2
from subprocess import call

client = MongoClient('localhost', 27017)
db = client['twitter_db']
tcol = db['tourist_tweets']
pcol = db['poi']
ucol = db['users']
scol = db['sequences']

def select(query):
    con = None
    result = None

    try:
        con = psycopg2.connect(database='eTurismo', user='postgres', password='postgres', host='localhost')

        cur = con.cursor()
        cur.execute(query)

        result = cur.fetchall()


    except psycopg2.DatabaseError as e:

        print("Error {}".format(e))

    finally:

        if con:
            con.close()

    return result


def create_place_dict():
    query = "SELECT poi_id, code_place FROM place"
    results = select(query)
    places = dict()
    for result in results:
        poi_id = result[0]
        code_place = result[1]
        places[poi_id] = code_place
    return places


def build_itemsets(distance, undefined=True, only_tourists=True):
    hash_table = dict()
    count = 0

    places = create_place_dict()

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
                        code_place = places.get(close_pois.next()['id'], -1)
                        if code_place == -1:
                            continue
                        closest_poi = code_place
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

    return hash_table


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


def decode(inv_map):
    with open("output/is_user.txt", "r") as f, open("decoded/is_user.txt", 'w') as g:
        for line in f:
            pattern = line.split(" #SUP: ")[0].split(" ")
            for i in range(len(pattern)):
                pattern[i] = str(inv_map.get(int(pattern[i]), 0))

            g.write(" ".join(pattern) + "\n")

def run_spmf():

    args = ["java", "-jar", "../tools/spmf.jar", "run", "Eclat", "input/is_user.txt", "output/is_user.txt", "1%"]
    call(args)



# Extract the encoded itemsets using a sequential numbering and store the map seq_num -> id
map = build_itemsets(15, undefined=False, only_tourists=True)
# Invert the key-values of the map (value -> key) to use later for decoding
inv_map = {v: k for k, v in map.items()}
# Extract the frequent patterns
run_spmf()
# Decode the frequent patterns to obtain the real id instead of the sequential number
decode(inv_map)


