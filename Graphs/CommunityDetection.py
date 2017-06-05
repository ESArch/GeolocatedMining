import community
import networkx as nx
import matplotlib.pyplot as plt
from random import random


#better with karate_graph() as defined in networkx example.
#erdos renyi don't have true community structure
# G = nx.erdos_renyi_graph(30, 0.05)
G = nx.read_gml('../MovieLens/graphs/g5.gml')

#first compute the best partition
partition = community.best_partition(G)

#drawing

plt.figure(figsize=(50,50))
plt.title("Community", fontsize=48)


size = float(len(set(partition.values())))
pos = nx.spring_layout(G)
count = 0.

labels = dict()
for node in G:
    if len(G[node]) >= 5:
        labels[node] = node

for com in set(partition.values()) :
    count = count + 1.
    list_nodes = [nodes for nodes in partition.keys()
                                if partition[nodes] == com]

    color = (random(), random(), random())
    nx.draw_networkx_nodes(G, pos, list_nodes, node_size = [len(G[node])*20 for node in list_nodes], node_color = color)


nx.draw_networkx_edges(G,pos, alpha=0.5)
nx.draw_networkx_labels(G,pos, labels)
# plt.show()
plt.savefig("community.pdf", dpi=1000, facecolor='w', edgecolor='w', orientation='portrait', papertype=None, format=None,
                transparent=False, bbox_inches=None, pad_inches=0.1)