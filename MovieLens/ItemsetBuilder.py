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


def select_movies_by_user(user_id, min_rating):
    query = "SELECT movie_title, rating FROM rating WHERE user_id = {} AND rating >= {}".format(user_id, min_rating)
    result = select(query)

def select_genres_by_movie(movie_id):
    query = "SELECT genre_id FROM movie_genre WHERE movie_id = {}".format(movie_id)
    result = select(query)
    return[(int(x[0]), genres[int(x[0])]) for x in result]

def select_users_by_movie(movie_id, min_rating):
    query = "SELECT user_id FROM rating WHERE movie_id = {} AND rating_value >= {}".format(movie_id, min_rating)
    result = select(query)
    return[(int(x[0]),) for x in result]

def select_gtags_by_movie(movie_id, min_relevance):
    query = "SELECT gtag_id FROM gtag_score WHERE movie_id = {} AND gs_relevance >= {}".format(movie_id, min_relevance)
    result = select(query)
    return[(int(x[0]),) for x in result]



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

    prev_size = 0
    for entry in result:
        id = entry[0]
        title = entry[1]
        movies[id] = title

    return movies


def build_genre_dict():

    genres = dict()

    query = "SELECT genre_id, genre_name FROM genre"
    result = select(query)

    for entry in result:
        id = entry[0]
        name = entry[1]
        genres[id] = name

    return genres


def build_gtag_dict():
    gtags = dict()

    query = "SELECT gtag_id, gtag_tag FROM gtag"
    result = select(query)

    for entry in result:
        id = entry[0]
        tag = entry[1]
        gtags[id] = tag

    return gtags


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

def build_graphs():

    with open("decoded.txt", "r") as f:
        for line in f:
            pattern = line.strip().split(" ")
            for element in pattern:
                build_graph((int(element), movies[(int(element))]), "movie", set(), set(), 3)
                break

def build_graph(node, node_type, nodes, edges, distance, ):
    if distance == 0:
        return nodes, edges

    if node_type == "movie":

        movie_id = node[0]


        new_nodes = select_genres_by_movie(movie_id)
        edges.update( [(node, new_node) for new_node in new_nodes] )
        nodes.update(new_nodes)
        for new_node in new_nodes:
           nodes, edges = build_graph(new_node, "genre", nodes, edges, distance - 1)

        new_nodes = select_users_by_movie(movie_id, 3)
        edges.update([(node, new_node) for new_node in new_nodes])
        nodes.update(new_nodes)
        for new_node in new_nodes:
           nodes, edges = build_graph(new_node, "user", nodes, edges, distance - 1)

        new_nodes = select_gtags_by_movie(movie_id, 0.5)
        edges.update([(node, new_node) for new_node in new_nodes])
        nodes.update(new_nodes)
        for new_node in new_nodes:
            nodes, edges = build_graph(new_node, "gtag", nodes, edges, distance - 1)

        pass
    elif node_type == "genre":
        # get_movies()
        pass
    elif node_type == "gtag":
        # get_movies()
        pass
    elif node_type == "user":
        # get_movies()
        pass

    return nodes, edges


movies = build_movie_dict()
genres = build_genre_dict()
gtags = build_gtag_dict()

build_graphs()