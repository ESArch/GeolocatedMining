import networkx as nx
import numpy as np
import Graphs.GraphSimilarity as gs
from operator import itemgetter
import psycopg2
import scipy

num_patterns = 52
num_communities = 16
num_places = 65
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


def compute_similarity_matrix():

    try:
        similarities = np.load("sim.npy")
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

    np.save("sim.npy", similarities)
    return similarities



def compute_significance_matrix():

    significances = np.zeros((num_places,num_communities))
    community_edges = list()
    for i in range(num_communities):
        g = nx.read_gml("communities/c{}.gml".format(i))
        community_edges.append(set(g.edges()))

    for i in range(num_places):
        vector = np.zeros(num_communities)
        g = nx.read_gml("place_graphs/g{}.gml".format(i))
        edges = set(g.edges())
        for j in range(num_communities):
            common_edges = set(edges).intersection(community_edges[j])
            vector[j] = len(common_edges) / len(community_edges[j])
        significances[i] = vector


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



    transaction_contexts = np.zeros((num_transactions, num_communities))

    places = build_places_dict()


    with open("decoded_itemsets.txt", "r") as f:
        count = 0

        for line in f:

            if count % 100 == 0:
                print(count)

            vector = np.zeros(num_communities)
            pattern = line.strip().split(" ")
            num_elements = 0
            for element in pattern:
                place_id = places[eval(element)]
                vector += significances[place_id]
                num_elements += 1

            vector /= num_elements
            transaction_contexts[count] = vector

            count += 1

    print(transaction_contexts)
    np.save("transaction_contexts.npy", transaction_contexts)
    return transaction_contexts


def compute_context(g, community_edges):

    vector = np.zeros((1, num_communities))
    edges = set(g.edges())

    for j in range(num_communities):
        common_edges = set(edges).intersection(community_edges[j])
        vector[0,j] = len(common_edges) / len(community_edges[j])

    return vector



def analyze():

    similarities = compute_similarity_matrix()
    transaction_contexts = compute_transaction_context()
    places = build_places_dict()

    community_edges = list()
    for i in range(num_communities):
        g = nx.read_gml("communities/c{}.gml".format(i))
        community_edges.append(set(g.edges()))


    with open("eTurismo.txt", "w", encoding="utf8") as f, open("decoded_itemsets.txt", "r") as g:

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


            # Compute similar patterns
            sim_graph_indexes = reversed(similarities[i].argsort())
            sim_graphs = list()
            for j in sim_graph_indexes:
                if similarities[i][j] >= 0.:
                    g2 = nx.read_gml("graphs/g{}.gml".format(j))
                    sim_graphs.append("{} ({})".format(g2.name, similarities[i][j]))


            # Compute relevant transactions
            context_vector = compute_context(g, community_edges)

            distances = scipy.spatial.distance.cdist(transaction_contexts, context_vector, 'cosine')
            distances = np.reshape(distances, distances.shape[0])

            sig_transactions_indexes = distances.argsort()[:5]
            significant_transactions = list()
            for j in sig_transactions_indexes:
                transaction = transactions[j].strip().split(" ")
                places_list = list()
                for item in transaction:
                    places_list.append(places[eval(item)])

                significant_transactions.append((distances[j], places_list))


            f.write("PATTERN: {}\n".format(g.name))
            f.write("Context indicators:\n")
            f.write("\t" + " | ".join(relevant_nodes) + "\n")
            f.write("Similar patterns: {} synonims\n".format(len(sim_graphs)))
            f.write("\t" + " | ".join(sim_graphs) + "\n")
            f.write("Transactions:\n")
            for j in range(len(significant_transactions)):
                f.write("\tDistance: {} Transaction: ".format(significant_transactions[j][0]) + " | ".join(
                    significant_transactions[j][1]) + "\n")
            f.write("\n")



analyze()