import networkx as nx
import numpy as np
import Graphs.GraphSimilarity as gs
from operator import itemgetter

def compute_similarity_matrix():

    try:
        similarities = np.load("sim3.npy")
    except:
        similarities = np.zeros((276, 276))

    with open("last_row.txt", "r") as f:
        last_row_computed = eval(f.read())

    for i in range(last_row_computed, 276):
        print("Computing similarities for node {}...".format(i))
        g1 = nx.read_gml("graphs/g{}.gml".format(i))
        similarity_row = np.zeros((1, 276))
        for j in range(i, 276):
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

compute_similarity_matrix()

similarities = np.load("sim5.npy")

# print(similarities)
with open("MovieLens5.txt", "w", encoding="utf8") as f:
    for i in range(276):
        g = nx.read_gml("graphs/g{}.gml".format(i))
        # Rank nodes using Pagerank algorithm
        ranked_nodes = g.degree()
        # Sort the nodes by relevance
        nodes_by_rank = sorted(ranked_nodes, key=ranked_nodes.get, reverse=True)
        # Top 10 relevant nodes
        relevant_nodes = nodes_by_rank[:10]

        sim_graph_indexes = reversed(similarities[i].argsort())
        sim_graphs = list()
        for j in sim_graph_indexes:
            if similarities[i][j] >= 0.:
                g2 = nx.read_gml("graphs/g{}.gml".format(j))
                sim_graphs.append("{} ({})".format(g2.name, similarities[i][j]))

        f.write("PATTERN: {}\n".format(g.name))
        f.write("Indicators:\n")
        f.write("\t" + " | ".join(relevant_nodes)+ "\n")
        f.write("Similar patterns: {} synonims\n".format(len(sim_graphs)))
        f.write("\t" + " | ".join(sim_graphs) + "\n")
        f.write("\n")



