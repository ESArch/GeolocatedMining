from operator import itemgetter

import psycopg2
import networkx as nx
import matplotlib.pyplot as plt
from networkx.drawing.nx_agraph import graphviz_layout
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

def build_graphs(distance, min_rating, min_relevance):

    count = 0
    with open("decoded.txt", "r") as f, open("pr.txt", "w") as out:
        for line in f:
            g = nx.Graph()
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
                nodes, edges = build_graph_lite(first_node, "movie", nodes, set(), distance, min_rating, min_relevance)

                # Keep only the names
                node_list += [x[1] for x in nodes]
                edge_list += [(x[0][1], x[1][1], x[2]) for x in edges]


            # Build the networkx.Graph
            g = nx.Graph()
            g.add_nodes_from(node_list)
            g.add_weighted_edges_from(edge_list)

            print(nx.info(g))

            g_filename = "graphs/g{}.gml".format(count)
            nx.write_gml(g, g_filename)

            count += 1


            # # Rank nodes using Pagerank algorithm
            # ranked_nodes = nx.pagerank(g).items()
            # # Sort the nodes by relevance
            # nodes_by_rank = sorted(ranked_nodes, key=itemgetter(1), reverse=True)
            # # Top 10 relevant nodes
            # relevant_nodes = nodes_by_rank[:10]
            # # Remove the relevance
            # relevant_nodes_labels = [x[0] for x in relevant_nodes]

            # Find the edges linking 2 relevant nodes
            # relevant_edges = list()
            # for edge in edge_list:
            #     if edge[0] in relevant_nodes_labels and edge[1] in relevant_nodes_labels:
            #         relevant_edges.append(edge)

            # Build a graph with the relevant nodes
            # gpr = nx.Graph()
            # gpr.add_weighted_edges_from(relevant_edges)

            # nx.draw(g, with_labels=True)
            # plt.show()

            # out.write("Pattern: {}\n".format(" ".join(movies_in_pattern)))
            # out.write(" ".join(str(s) for s in relevant_nodes))
            # out.write("\n\n")
            # out.flush()

            # break



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


def print_labels():
    count = 0
    with open("decoded.txt", "r") as f, open("labels.txt", "w") as g:
        for line in f:
            pattern = line.strip().split(" ")
            pattern_labels = [movies[int(x)] for x in pattern]
            print(pattern_labels)

            g.write("{}: ".format(count) + " ".join(pattern_labels) + "\n")
            count += 1


movies = build_movie_dict()
genres = build_genre_dict()
gtags = build_gtag_dict()

# build_graphs(3, 4, 0.95)






