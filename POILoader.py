import psycopg2
import pymongo
import ast


conn = psycopg2.connect("dbname=template_postgis user=postgres password=postgres")
print("Connected ")


cur = conn.cursor()
query = "SELECT poi_nombre, poi_tipo_lugar, ST_ASGEOJSON(poi_poligono) FROM geoturismo.punto_interes"
cur.execute(query)

result = cur.fetchall()


client = pymongo.MongoClient('localhost', 27017)
db = client['twitter_db']
collection = db['poi']


for data in result:
    poi = {}
    poi['name'] = data[0]
    poi['category'] = data[1]
    poi['coordinates'] = ast.literal_eval(data[2])
    #print(poi)
    collection.insert(poi)