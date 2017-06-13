import networkx as nx
import numpy as np
import Graphs.GraphSimilarity as gs
from operator import itemgetter
import psycopg2
import scipy

num_patterns = 52
num_communities = 14
num_items = 65
num_transactions = 1062

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


def build_places_dict():

    places = dict()

    query = "SELECT code_place, name FROM place"
    result = select(query)

    for entry in result:
        id = int(entry[0])
        name = entry[1]
        places[id] = name

    return places


def build_item_to_row_map():

    items = dict()

    with open("data/places.txt", "r") as f:
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

    for i in range(num_patterns):
        g1 = nx.read_gml("graphs/g{}.gml".format(i))
        for j in range(i, num_patterns):

            g2 = nx.read_gml("graphs/g{}.gml".format(j))
            # sim = gs.laplacian_similarity(g1, g2)

            try:
                sim = gs.FaBP_similarity(g1, g2)
            except:
                sim = 0

            similarities[i, j] = sim
            similarities[j, i] = sim

    np.save("data/sim.npy", similarities)
    return similarities



def compute_item_context():

    try:
        significances = np.load("data/item_contexts.npy")
        print("Item contexts matrix loaded")
        return significances
    except:
        print("Computing item contexts matrix")

    significances = np.zeros((num_items,num_communities))
    community_edges = list()
    for i in range(num_communities):
        g = nx.read_gml("communities/c{}.gml".format(i))
        community_edges.append(set([tuple(sorted(edge)) for edge in g.edges()]))

    for i in range(num_items):
        vector = np.zeros(num_communities)
        g = nx.read_gml("place_graphs/g{}.gml".format(i))
        vector = compute_context(g, community_edges)
        edges = set(g.edges())
        # for j in range(num_communities):
        #     common_edges = set(edges).intersection(community_edges[j])
        #     vector[j] = len(common_edges) / len(community_edges[j])
        significances[i] = vector


    np.save("data/item_contexts.npy", significances)

    return significances


def compute_transaction_context():

    try:
        transaction_contexts = np.load("data/transaction_contexts.npy")
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

    print(transaction_contexts)
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
    places = build_places_dict()

    community_edges = list()
    for i in range(num_communities):
        g = nx.read_gml("communities/c{}.gml".format(i))
        community_edges.append(set([tuple(sorted(edge)) for edge in g.edges()]))


    with open("eTurismo.txt", "w", encoding="utf8") as f, open("data/decoded_itemsets.txt", "r") as g:

        transactions = g.readlines()

        for i in range(num_patterns):
            g = nx.read_gml("graphs/g{}.gml".format(i))
            # Rank nodes using Pagerank algorithm
            ranked_nodes = nx.pagerank(g).items()
            # Sort the nodes by relevance
            nodes_by_rank = sorted(ranked_nodes, key=itemgetter(1), reverse=True)
            # Top 10 relevant nodes
            relevant_nodes = nodes_by_rank[:10]
            # Remove the relevance
            relevant_nodes_labels = [x[0] for x in relevant_nodes]
            # sim_row = np.array([x if x >= 0.9 else 0. for x in similarities[i]])

            # context_vector = compute_context(g, community_edges)
            context_vector = pattern_contexts[i,:].reshape((1,num_communities))


            # Compute similar patterns

            # sim_graph_indexes = reversed(similarities[i].argsort())
            # sim_graphs = list()
            # for j in sim_patterns_indexes:
            #     if similarities[i][j] >= 0.:
            #         g2 = nx.read_gml("graphs/g{}.gml".format(j))
            #         sim_graphs.append("{} ({})".format(g2.name, similarities[i][j]))

            distances = scipy.spatial.distance.cdist(pattern_contexts, context_vector, 'cosine')
            distances = np.reshape(distances, distances.shape[0])
            sim_patterns_indexes = distances.argsort()
            sim_graphs = list()
            for j in sim_patterns_indexes:
                if 1-distances[j] >= .9:
                    g2 = nx.read_gml("graphs/g{}.gml".format(j))
                    sim_graphs.append("{} ({})".format(g2.name, 1-distances[j]))



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
                    places_list.append(places[eval(item)])

                significant_transactions.append((1-distances[j], places_list))


            f.write("PATTERN: {}\n".format(g.name))
            f.write("Context indicators:\n")
            f.write("\t" + " | ".join(relevant_nodes_labels) + "\n")
            f.write("Similar patterns: {} synonims\n".format(len(sim_graphs)))
            # f.write("\t" + " | ".join(sim_graphs) + "\n")
            for j in range(len(sim_graphs)):
                f.write("\t{}\n".format(sim_graphs[j]))
            f.write("Transactions:\n")
            for j in range(len(significant_transactions)):
                f.write("\tSimilarity: {} Transaction: ".format(significant_transactions[j][0]) + " | ".join(
                    significant_transactions[j][1]) + "\n")
            f.write("\n")



analyze()