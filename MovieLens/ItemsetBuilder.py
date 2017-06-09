from operator import itemgetter

import psycopg2
import networkx as nx
import matplotlib.pyplot as plt
from networkx.drawing.nx_agraph import graphviz_layout
from subprocess import call
import pandas as pd

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
    query = "SELECT movie_id, rating_value FROM rating WHERE user_id = {} AND rating_value >= {}".format(user_id, min_rating)
    #print(query)
    result = select(query)
    return [(int(x[0]), movies[int(x[0])], float(x[1])/5) for x in result]

def select_genres_by_movie(movie_id):
    query = "SELECT genre_id FROM movie_genre WHERE movie_id = {}".format(movie_id)
    #print(query)
    result = select(query)
    return[(int(x[0]), genres[int(x[0])], 1.0) for x in result]

def select_users_by_movie(movie_id, min_rating):
    query = "SELECT user_id, rating_value FROM rating WHERE movie_id = {} AND rating_value >= {} AND user_id <= 5000 ".format(movie_id, min_rating)
    #print(query)
    result = select(query)
    return[(int(x[0]), "USER#{}".format(str(int(x[0]))), float(x[1])/5) for x in result]

def select_gtags_by_movie(movie_id, min_relevance):
    query = "SELECT gtag_id, gs_relevance FROM gtag_score WHERE movie_id = {} AND gs_relevance >= {}".format(movie_id, min_relevance)
    #print(query)
    result = select(query)
    return[(int(x[0]), gtags[int(x[0])], float(x[1])) for x in result]

def select_movies_by_genre(genre_id):
    query = "SELECT movie_id FROM movie_genre WHERE genre_id = {}".format(genre_id)
    #print(query)
    result = select(query)
    return[(int(x[0]), movies[int(x[0])], 1.0) for x in result]

def select_movies_by_gtag(gtag_id, min_relevance):
    query = "SELECT movie_id, gs_relevance FROM gtag_score WHERE gtag_id = {} AND gs_relevance >= {}".format(gtag_id, min_relevance)
    #print(query)
    result = select(query)
    return[(int(x[0]), movies[int(x[0])], float(x[1])) for x in result]


def buildItemsets(min_rating):

    user_ratings = dict()
    user_movies = dict() # Same as user_ratings but NOT enconded
    hash_table = dict()
    count = 0

    query = "SELECT user_id, movie_id, rating_value FROM rating WHERE rating_value >= {} ORDER BY user_id".format(min_rating)
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
        movies = user_movies.get(user_id, [])
        movies += [movie_id]

        user_ratings[user_id] = ratings
        user_movies[user_id] = movies

    with open('itemsets.txt', 'w') as f:
        for key in user_ratings.keys():
            f.write(" ".join(list(map(str, sorted(user_ratings[key])))))
            f.write("\n")

    with open('decoded_itemsets.txt', 'w') as f:
        for key in user_movies.keys():
            f.write(" ".join(list(map(str, user_movies[key]))))
            f.write("\n")


    return hash_table

def run_spmf():

    args = ["java", "-jar", "../tools/spmf.jar", "run", "Eclat", "itemsets.txt", "output.txt", "10%"]
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


def build_gtag_score_table(min_relevance):
    query = "SELECT gtag_id, movie_id, gs_relevance FROM gtag_score WHERE gs_relevance >= {}".format(min_relevance)
    result = select(query)

    tuples = [(x[0], x[1]) for x in result]
    values = [x[2] for x in result]

    index = pd.MultiIndex.from_tuples(tuples, names=['gtag_id', 'movie_id'])

    s = pd.Series(values, index=index)

    return s

def decode(movie_map):
    with open("output.txt", "r") as f, open("decoded.txt", 'w') as g:
        for line in f:
            pattern = line.split(" #SUP: ")[0].split(" ")
            for i in range(len(pattern)):
                pattern[i] = str(movie_map.get(int(pattern[i]), 0))

            g.write(" ".join(pattern) + "\n")


def run():
    movie_hashtable = buildItemsets(4)
    inv_map = {v: k for k, v in movie_hashtable.items()}
    run_spmf()
    decode(inv_map)

def build_graphs(distance, min_rating, min_relevance, input_file, output_folder):

    count = 0
    with open(input_file, "r") as f, open("pr.txt", "w") as out:
        for line in f:
            node_list = list()
            edge_list = list()
            movies_in_pattern = list() # Pattern description



            pattern = line.strip().split(" ")
            for element in pattern:
                first_node = (int(element), movies[(int(element))])
                movies_in_pattern += [movies[(int(element))]]
                nodes = set()
                nodes.add(first_node)
                # nodes, edges = build_graph(first_node, "movie", nodes, set(), 2)
                # nodes, edges = build_graph_lite(first_node, "movie", nodes, set(), distance, min_rating, min_relevance)
                nodes, edges = build_graph_fast(first_node, "movie", nodes, set(), distance)

                # Keep only the names
                node_list += [x[1] for x in nodes]
                edge_list += [(x[0][1], x[1][1], x[2]) for x in edges]


            # Build the networkx.Graph
            g = nx.Graph()
            g.add_nodes_from(node_list)
            g.add_weighted_edges_from(edge_list)
            g.name = ", ".join(movies_in_pattern)

            print(nx.info(g))

            g_filename = "{}/g{}.gml".format(output_folder, count)
            nx.write_gml(g, g_filename)

            count += 1

            if count % 50 == 0:
                print(count)




def build_graph(node, node_type, nodes, edges, distance ):
    #print(node_type, distance)

    if distance == 0:
        return nodes, edges

    if node_type == "movie":

        movie_id = node[0]
        new_nodes = select_genres_by_movie(movie_id)
        # print(len(new_nodes))

        edges.update( [(node, (new_node[0], new_node[1]), new_node[2]) for new_node in new_nodes] )
        nodes.update([(new_node[0], new_node[1]) for new_node in new_nodes])
        for new_node in new_nodes:
           nodes, edges = build_graph(new_node, "genre", nodes, edges, distance - 1)


        new_nodes = select_users_by_movie(movie_id, 5)
        # print(len(new_nodes))

        edges.update([(node, (new_node[0], new_node[1]), new_node[2]) for new_node in new_nodes])
        nodes.update([(new_node[0], new_node[1]) for new_node in new_nodes])
        for new_node in new_nodes:
           nodes, edges = build_graph(new_node, "user", nodes, edges, distance - 1)

        new_nodes = select_gtags_by_movie(movie_id, 0.9)
        # print(len(new_nodes))

        edges.update([(node, (new_node[0], new_node[1]), new_node[2]) for new_node in new_nodes])
        nodes.update([(new_node[0], new_node[1]) for new_node in new_nodes])
        for new_node in new_nodes:
            nodes, edges = build_graph(new_node, "gtag", nodes, edges, distance - 1)


    elif node_type == "genre":
        genre_id = node[0]
        new_nodes = select_movies_by_genre(genre_id)
        edges.update([(node, (new_node[0], new_node[1]), new_node[2]) for new_node in new_nodes])
        nodes.update([(new_node[0], new_node[1]) for new_node in new_nodes])
        for new_node in new_nodes:
            nodes, edges = build_graph(new_node, "movie", nodes, edges, distance - 1)


    elif node_type == "gtag":
        gtag_id = node[0]
        new_nodes = select_movies_by_gtag(gtag_id, 0.9)
        edges.update([(node, (new_node[0], new_node[1]), new_node[2]) for new_node in new_nodes])
        nodes.update([(new_node[0], new_node[1]) for new_node in new_nodes])
        for new_node in new_nodes:
            nodes, edges = build_graph(new_node, "movie", nodes, edges, distance - 1)


    elif node_type == "user":
        user_id = node[0]
        new_nodes = select_movies_by_user(user_id, 5)
        # edges.update([(node, new_node) for new_node in new_nodes])
        # nodes.update(new_nodes)
        edges.update([(node, (new_node[0], new_node[1]), new_node[2]) for new_node in new_nodes])
        nodes.update([(new_node[0], new_node[1]) for new_node in new_nodes])
        for new_node in new_nodes:
            nodes, edges = build_graph(new_node, "movie", nodes, edges, distance - 1)


    return nodes, edges


def build_graph_lite(node, node_type, nodes, edges, distance, min_rating, min_relevance ):

    if distance == 0:
        return nodes, edges


    if node_type == "movie":

        movie_id = node[0]

        found_nodes = select_gtags_by_movie(movie_id, min_relevance)
        found_nodes = set(found_nodes)
        new_nodes = found_nodes.difference(nodes)

        # print("Found {} tags for movie {}".format(len(new_nodes), movies[movie_id]))

        edges.update([(node, (new_node[0], new_node[1]), new_node[2]) for new_node in new_nodes])
        nodes.update([(new_node[0], new_node[1]) for new_node in new_nodes])

        for new_node in new_nodes:
            nodes, edges = build_graph_lite(new_node, "gtag", nodes, edges, distance - 1, min_rating, min_relevance)



    elif node_type == "gtag":
        gtag_id = node[0]

        found_nodes = select_movies_by_gtag(gtag_id, min_relevance)
        found_nodes = set(found_nodes)
        new_nodes = found_nodes.difference(nodes)

        # print("Found {} movies for tag {}".format(len(new_nodes), gtags[gtag_id]))

        edges.update([(node, (new_node[0], new_node[1]), new_node[2]) for new_node in new_nodes])
        nodes.update([(new_node[0], new_node[1]) for new_node in new_nodes])
        for new_node in new_nodes:
            nodes, edges = build_graph_lite(new_node, "movie", nodes, edges, distance - 1, min_rating, min_relevance)


    return nodes, edges


def build_graph_fast(node, node_type, nodes, edges, distance):

    if distance == 0:
        return nodes, edges


    if node_type == "movie":

        movie_id = node[0]

        try:
            found_gtags = gtag_score[:, movie_id].to_dict().items()
        except:
            found_gtags = []
        found_nodes = [(x[0], gtags[x[0]], x[1]) for x in found_gtags]
        found_nodes = set(found_nodes)
        new_nodes = found_nodes.difference(nodes)

        # print("Found {} tags for movie {}".format(len(new_nodes), movies[movie_id]))

        edges.update([(node, (new_node[0], new_node[1]), new_node[2]) for new_node in new_nodes])
        nodes.update([(new_node[0], new_node[1]) for new_node in new_nodes])

        for new_node in new_nodes:
            nodes, edges = build_graph_fast(new_node, "gtag", nodes, edges, distance - 1)



    elif node_type == "gtag":
        gtag_id = node[0]

        try:
            found_movies = gtag_score[gtag_id, :].to_dict().items()
        except:
            found_movies = []

        found_nodes = [(x[0], movies[x[0]], x[1]) for x in found_movies]
        found_nodes = set(found_nodes)
        new_nodes = found_nodes.difference(nodes)

        # print("Found {} movies for tag {}".format(len(new_nodes), gtags[gtag_id]))

        edges.update([(node, (new_node[0], new_node[1]), new_node[2]) for new_node in new_nodes])
        nodes.update([(new_node[0], new_node[1]) for new_node in new_nodes])
        for new_node in new_nodes:
            nodes, edges = build_graph_fast(new_node, "movie", nodes, edges, distance - 1)


    return nodes, edges



def print_labels():
    count = 0
    with open("decoded.txt", "r") as f, open("labels.txt", "w") as g:
        for line in f:
            pattern = line.strip().split(" ")
            pattern_labels = [movies[int(x)] for x in pattern]
            print(pattern_labels)

            g.write("{}: ".format(count) + " ".join(pattern_labels) + "\n")
            count += 1



# run()

movies = build_movie_dict()
genres = build_genre_dict()
gtags = build_gtag_dict()
gtag_score = build_gtag_score_table(0.95)

# print_labels()

# build_graphs(3, 4, 0.95, "decoded.txt", "graphs")
build_graphs(3, 4, 0.95, "movies.txt", "movie_graphs")