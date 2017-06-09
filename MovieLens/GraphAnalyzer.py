import networkx as nx
import numpy as np
import Graphs.GraphSimilarity as gs
from operator import itemgetter
import scipy
import psycopg2

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

def compute_similarity_matrix():

    num_patterns = 476

    try:
        similarities = np.load("sim5.npy")
        print("Similarity matrix loaded")
        return similarities
    except:
        print("Computing similarity matrix")

    similarities = np.zeros((num_patterns, num_patterns))

    with open("last_row.txt", "r") as f:
        last_row_computed = eval(f.read())

    for i in range(last_row_computed, num_patterns):
        print("Computing similarities for node {}...".format(i))
        g1 = nx.read_gml("graphs/g{}.gml".format(i))
        similarity_row = np.zeros((1, num_patterns))
        for j in range(i, num_patterns):
            print(j, end=" ", flush=True)
            g2 = nx.read_gml("graphs/g{}.gml".format(j))
            # sim = gs.laplacian_similarity(g1, g2)
            try:
                sim = gs.FaBP_similarity(g1, g2)
            except:
                sim = 0

            similarities[i, j] = sim
            similarities[j, i] = sim

        np.save("sim5.npy", similarities)
        print(similarities[i])

        with open("last_row.txt", "w") as f:
            f.write(str(i))

        print("Similarities for node {} stored".format(i))


def compute_significance_matrix():
    num_movies = 27278
    num_communities = 38
    significances = np.zeros((num_movies,num_communities))
    community_edges = list()
    for i in range(num_communities):
        g = nx.read_gml("communities/c{}.gml".format(i))
        community_edges.append(set(g.edges()))

    for i in range(num_movies):
        vector = np.zeros(num_communities)
        g = nx.read_gml("movie_graphs/g{}.gml".format(i))
        edges = set(g.edges())
        for j in range(num_communities):
            common_edges = set(edges).intersection(community_edges[j])
            vector[j] = len(common_edges) / len(community_edges[j])
        significances[i] = vector
        if i % 50 == 0:
            print(i)

    np.save("sig.npy", significances)

    return significances

def compute_transaction_context():

    try:
        transaction_contexts = np.load("transaction_contexts.npy")
        print("Transaction contexts loaded")
        return transaction_contexts
    except:
        print("Computing transaction contexts")


    try:
        significances = np.load("sig.npy")
    except:
        significances = compute_significance_matrix()


    num_transactions = 138287
    num_communities = 38

    transaction_contexts = np.zeros((num_transactions, num_communities))

    movies = build_movie_dict()


    with open("decoded_itemsets.txt", "r") as f:
        count = 0

        for line in f:

            if count % 100 == 0:
                print(count)

            vector = np.zeros(num_communities)
            pattern = line.strip().split(" ")
            num_elements = 0
            for element in pattern:
                movie_id = movies[eval(element)]
                vector += significances[movie_id]
                num_elements += 1

            vector /= num_elements
            transaction_contexts[count] = vector

            count += 1

    print(transaction_contexts)
    np.save("transaction_contexts.npy", transaction_contexts)
    return transaction_contexts


def compute_context(g, community_edges):
    num_communities = len(community_edges)

    vector = np.zeros((1, num_communities))
    edges = set(g.edges())

    for j in range(num_communities):
        common_edges = set(edges).intersection(community_edges[j])
        vector[0,j] = len(common_edges) / len(community_edges[j])

    return vector



def analyze():

    similarities = compute_similarity_matrix()
    transaction_contexts = compute_transaction_context()
    movies = build_movie_dict()

    num_communities = 38
    community_edges = list()
    for i in range(num_communities):
        g = nx.read_gml("communities/c{}.gml".format(i))
        community_edges.append(set(g.edges()))

    with open("MovieLens5.txt", "w", encoding="utf8") as f, open("decoded_itemsets.txt", "r") as g:

        transactions = g.readlines()

        for i in range(476):
            g = nx.read_gml("graphs/g{}.gml".format(i))
            # Rank nodes using Pagerank algorithm
            ranked_nodes = g.degree()
            # Sort the nodes by relevance
            nodes_by_rank = sorted(ranked_nodes, key=ranked_nodes.get, reverse=True)
            # Top 10 relevant nodes
            relevant_nodes = nodes_by_rank[:10]

            # Compute similar patterns
            # sim_graph_indexes = reversed(similarities[i].argsort())
            sim_graphs = list()
            # for j in sim_graph_indexes:
            #     if similarities[i][j] >= 0.:
            #         g2 = nx.read_gml("graphs/g{}.gml".format(j))
            #         sim_graphs.append("{} ({})".format(g2.name, similarities[i][j]))


            # Compute relevant transactions
            context_vector = compute_context(g, community_edges)

            distances = scipy.spatial.distance.cdist(transaction_contexts, context_vector, 'cosine')
            distances = np.reshape(distances, distances.shape[0])


            sig_transactions_indexes = distances.argsort()[:5]
            significant_transactions = list()
            for j in sig_transactions_indexes:
                transaction = transactions[j].strip().split(" ")
                movie_list = list()
                for item in transaction:
                    movie_list.append(movies[eval(item)])

                significant_transactions.append((distances[j], movie_list))





            f.write("PATTERN: {}\n".format(g.name))
            f.write("Context indicators:\n")
            f.write("\t" + " | ".join(relevant_nodes) + "\n")
            f.write("Similar patterns: {} synonims\n".format(len(sim_graphs)))
            f.write("\t" + " | ".join(sim_graphs) + "\n")
            f.write("Transactions:\n")
            for j in range(len(significant_transactions)):
                f.write("\tDistance: {} Transaction: ".format(significant_transactions[j][0]) + " | ".join(significant_transactions[j][1]) + "\n" )
            f.write("\n")


# compute_similarity_matrix()
# compute_transaction_context()

# similarities = np.load("sim5.npy")


analyze()





