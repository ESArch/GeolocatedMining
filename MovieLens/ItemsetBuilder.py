import psycopg2
from subprocess import call

def select(query):
    con = None
    result = None

    try:
        con = psycopg2.connect(database='MovieLens', user='postgres', password='postgres', host='localhost')

        cur = con.cursor()
        cur.execute(query)

        result = cur.fetchall()


    except psycopg2.DatabaseError as e:

        print("Error {}".format(e))

    finally:

        if con:
            con.close()

    return result


def buildItemsets(min_rating):

    user_ratings = dict()
    hash_table = dict()
    count = 0

    query = "SELECT user_id, movie_id, rating_value FROM rating WHERE rating_value <= {} ORDER BY user_id".format(min_rating)
    result = select(query)


    for entry in result:
        user_id = entry[0]
        movie_id = entry[1]
        movie_hash = hash_table.get(movie_id, 0)
        if movie_hash == 0:
            count += 1
            hash_table[movie_id] = count
            movie_hash = count

        ratings = user_ratings.get(user_id, [])
        ratings += [movie_hash]
        user_ratings[user_id] = ratings

    with open('itemsets.txt', 'w') as f:
        for key in user_ratings.keys():
            f.write(" ".join(list(map(str, sorted(user_ratings[key])))))
            f.write("\n")

    return hash_table

def run_spmf():

    args = ["java", "-jar", "../tools/spmf.jar", "run", "Eclat", "itemsets.txt", "output.txt", "5%"]
    call(args)

def build_movie_dict():

    movies = dict()

    query = "SELECT movie_id, movie_title FROM movie"
    result = select(query)

    for entry in result:
        id = result[0]
        title = result[1]
        movies[id] = title

    return movies

def decode(movie_map):
    with open("output.txt", "r") as f, open("decoded.txt", 'w') as g:
        for line in f:
            pattern = line.split(" #SUP: ")[0].split(" ")
            for i in range(len(pattern)):
                pattern[i] = str(movie_map.get(int(pattern[i]), 0))

            g.write(" ".join(pattern) + "\n")


def run():
    movie_hashtable = buildItemsets(3)
    inv_map = {v: k for k, v in movie_hashtable.items()}
    run_spmf()
    decode(inv_map)

    movies = build_movie_dict()



run()