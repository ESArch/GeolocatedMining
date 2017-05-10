import psycopg2
import pymongo
import ast


conn = psycopg2.connect("dbname=template_postgis user=postgres password=postgres")
print("Connected ")


cur = conn.cursor()
query = "SELECT poi_id, poi_nombre, poi_tipo_lugar, ST_ASGEOJSON(poi_poligono) FROM geoturismo.punto_interes"
cur.execute(query)

result = cur.fetchall()


client = pymongo.MongoClient('localhost', 27017)
db = client['twitter_db']
collection = db['poi']


for data in result:
    poi = {}
    poi['id'] = data[0]
    poi['name'] = data[1]
    poi['category'] = data[2]
    poi['coordinates'] = ast.literal_eval(data[3])
    #print(poi)
    collection.insert(poi)