import networkx as nx
import numpy as np
import Graphs.GraphSimilarity as gs
from operator import itemgetter
import scipy
import psycopg2


num_patterns = 476
num_communities = 38
num_items = 27278
num_transactions = 138287


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

    for entry in result:
        id = entry[0]
        title = entry[1]
        movies[id] = title

    return movies


def build_item_to_row_map():

    items = dict()

    with open("data/items.txt", "r") as f:
        count = 0
        for line in f:
            item_id = eval(line)
            items[item_id] = count
            count += 1

    return items

def compute_similarity_matrix():

    try:
        similarities = np.load("data/sim.npy")
        print("Similarity matrix loaded")
        return similarities
    except:
        print("Computing similarity matrix")

    similarities = np.zeros((num_patterns, num_patterns))

    with open("data/last_row.txt", "r") as f:
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

        np.save("data/sim.npy", similarities)
        print(similarities[i])

        with open("data/last_row.txt", "w") as f:
            f.write(str(i))

        print("Similarities for node {} stored".format(i))

    return similarities


def compute_item_context():

    try:
        item_contexts = np.load("data/item_contexts.npy")
        print("Item context matrix loaded")
        return item_contexts
    except:
        print("Computing item contexts matrix")

        item_contexts = np.zeros((num_items,num_communities))
    community_edges = list()
    for i in range(num_communities):
        g = nx.read_gml("communities/c{}.gml".format(i))
        community_edges.append(set([tuple(sorted(edge)) for edge in g.edges()]))

    for i in range(num_items):
        g = nx.read_gml("item_graphs/g{}.gml".format(i))
        vector = compute_context(g, community_edges)
        item_contexts[i] = vector
        if i % 50 == 0:
            print(i)

    np.save("data/item_contexts.npy", item_contexts)

    return item_contexts

def compute_transaction_context():

    try:
        transaction_contexts = np.load("transaction_contexts.npy")
        print("Transaction contexts loaded")
        return transaction_contexts
    except:
        print("Computing transaction contexts")


    item_contexts = compute_item_context()
    transaction_contexts = np.zeros((num_transactions, num_communities))

    items = build_item_to_row_map()


    with open("data/decoded_itemsets.txt", "r") as f:
        count = 0

        for line in f:

            if count % 100 == 0:
                print(count)

            vector = np.zeros(num_communities)
            pattern = line.strip().split(" ")
            num_elements = 0
            for element in pattern:
                item_id = items[eval(element)]
                vector += item_contexts[item_id]
                num_elements += 1

            vector /= num_elements
            transaction_contexts[count] = vector

            count += 1

    np.save("data/transaction_contexts.npy", transaction_contexts)
    return transaction_contexts


def compute_pattern_contexts():
    try:
        pattern_contexts = np.load("data/pattern_contexts.npy")
        print("Patterns contexts loaded")
        return pattern_contexts
    except:
        print("Computing patterns contexts")

    community_edges = list()
    for i in range(num_communities):
        g = nx.read_gml("communities/c{}.gml".format(i))
        community_edges.append(set([tuple(sorted(edge)) for edge in g.edges()]))

    pattern_contexts = np.zeros((num_patterns, num_communities))

    for i in range(num_patterns):
        g = nx.read_gml("graphs/g{}.gml".format(i))
        pattern_contexts[i] = compute_context(g, community_edges)

    np.save("data/pattern_contexts.npy", pattern_contexts)
    return pattern_contexts


def compute_context(g, community_edges):

    vector = np.zeros((1, num_communities))
    edges = set(g.edges())
    edges = [tuple(sorted(edge)) for edge in edges]

    for j in range(num_communities):
        common_edges = set(edges)
        common_edges = common_edges.intersection(community_edges[j])
        num_common_edges = len(common_edges)
        num_community_edges = len(community_edges[j])
        vector[0,j] = num_common_edges / num_community_edges

    return vector



def analyze():

    # similarities = compute_similarity_matrix()
    transaction_contexts = compute_transaction_context()
    pattern_contexts = compute_pattern_contexts()
    movies = build_movie_dict()

    community_edges = list()
    for i in range(num_communities):
        g = nx.read_gml("communities/c{}.gml".format(i))
        community_edges.append(set([tuple(sorted(edge)) for edge in g.edges()]))

    with open("MovieLens.txt", "w", encoding="utf8") as f, open("data/decoded_itemsets.txt", "r") as g:

        transactions = g.readlines()

        for i in range(num_patterns):
            g = nx.read_gml("graphs/g{}.gml".format(i))
            # Rank nodes using Pagerank algorithm
            ranked_nodes = g.degree()
            # Sort the nodes by relevance
            nodes_by_rank = sorted(ranked_nodes, key=ranked_nodes.get, reverse=True)
            # Top 10 relevant nodes
            relevant_nodes = nodes_by_rank[:10]


            context_vector = pattern_contexts[i,:].reshape(1, num_communities)


            # Compute similar patterns

            # sim_graph_indexes = reversed(similarities[i].argsort())
            # sim_graphs = list()
            # for j in sim_graph_indexes:
            #     if similarities[i][j] >= 0.:
            #         g2 = nx.read_gml("graphs/g{}.gml".format(j))
            #         sim_graphs.append("{} ({})".format(g2.name, similarities[i][j]))

            distances = scipy.spatial.distance.cdist(pattern_contexts, context_vector, 'cosine')
            distances = np.reshape(distances, distances.shape[0])
            sim_patterns_indexes = distances.argsort()
            sim_graphs = list()
            for j in sim_patterns_indexes:
                if 1 - distances[j] >= .9:
                    g2 = nx.read_gml("graphs/g{}.gml".format(j))
                    sim_graphs.append("{} ({})".format(g2.name, 1 - distances[j]))


            # Compute relevant transactions
            distances = scipy.spatial.distance.cdist(transaction_contexts, context_vector, 'cosine')
            distances = np.reshape(distances, distances.shape[0])

            sig_transactions_indexes = distances.argsort()[:5]
            significant_transactions = list()
            for j in sig_transactions_indexes:
                transaction = transactions[j].strip().split(" ")
                transaction_context = transaction_contexts[j]
                places_list = list()
                for item in transaction:
                    places_list.append(movies[eval(item)])

                significant_transactions.append((1 - distances[j], places_list))


            f.write("PATTERN: {}\n".format(g.name))
            f.write("Context indicators:\n")
            f.write("\t" + " | ".join(relevant_nodes) + "\n")
            f.write("Similar patterns: {} synonims\n".format(len(sim_graphs)))
            # f.write("\t" + " | ".join(sim_graphs) + "\n")
            for j in range(len(sim_graphs)):
                f.write("\t{}\n".format(sim_graphs[j]))
            f.write("Transactions:\n")
            for j in range(len(significant_transactions)):
                f.write("\tSimilarity: {} Transaction: ".format(significant_transactions[j][0]) + " | ".join(
                    significant_transactions[j][1]) + "\n")
            f.write("\n")


# compute_similarity_matrix()
# compute_transaction_context()

# similarities = np.load("sim5.npy")


analyze()





