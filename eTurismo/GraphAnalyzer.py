import networkx as nx
import numpy as np
import Graphs.GraphSimilarity as gs
from operator import itemgetter

def compute_similarity_matrix():
    similarities = np.zeros((58, 58))

    for i in range(58):
        g1 = nx.read_gml("graphs/g{}.gml".format(i))
        for j in range(i, 58):
            # print(i, j)
            g2 = nx.read_gml("graphs/g{}.gml".format(j))
            # sim = gs.laplacian_similarity(g1, g2)

            try:
                sim = gs.FaBP_similarity(g1, g2)
            except:
                sim = 0

            similarities[i, j] = sim
            similarities[j, i] = sim

    np.save("sim5.npy", similarities)






# compute_similarity_matrix()
# compute_significance_matrix()

similarities = np.load("sim5.npy")

with open("eTurismo5.txt", "w", encoding="utf8") as f:
    for i in range(58):
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
        sim_graph_indexes = reversed(similarities[i].argsort())
        sim_graphs = list()
        for j in sim_graph_indexes:
            if similarities[i][j] >= 0.:
                g2 = nx.read_gml("graphs/g{}.gml".format(j))
                sim_graphs.append("{} ({})".format(g2.name, similarities[i][j]))

        f.write("PATTERN: {}\n".format(g.name))
        f.write("Indicators:\n")
        f.write("\t" + " | ".join(relevant_nodes_labels) + "\n")
        f.write("Similar patterns: {} synonims\n".format(len(sim_graphs)))
        f.write("\t" + " | ".join(sim_graphs) + "\n")
        f.write("\n")



