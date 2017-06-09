import psycopg2
import networkx as nx
import pandas as pd
import community
from random import random
import matplotlib.pyplot as plt

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

def build_place_dict():

    places = dict()

    query = "SELECT code_place, name FROM place"
    result = select(query)

    for entry in result:
        id = int(entry[0])
        name = entry[1]
        places[id] = name

    return places


def build_preference_dict():

    preferences = dict()

    query = "SELECT id, name FROM preferences"
    result = select(query)

    for entry in result:
        id = int(entry[0])
        name = entry[1]
        preferences[id] = name

    return preferences

def build_place_preferences_table():
    query = "SELECT id_preferences, code_place, value_interese FROM place_preferences"
    result = select(query)

    tuples = [(int(x[0]), int(x[1])) for x in result]
    values = [float(x[2]) for x in result]

    index = pd.MultiIndex.from_tuples(tuples, names=['id_preferences', 'code_place'])

    s = pd.Series(values, index=index)

    return s


def build_community_graph():
    places = build_place_dict()
    preferences = build_preference_dict()
    place_preferences = build_place_preferences_table()

    edges = list()
    for k,v in place_preferences.to_dict().items():
        edge = (preferences[k[0]], places[k[1]], v)
        edges.append(edge)

    g = nx.Graph()
    g.add_weighted_edges_from(edges)

    return g


try:
    g = nx.read_gml("communities/community_graph.gml")
    print("Reading graph...")
except:
    print("Building graph...")
    g = build_community_graph()
    nx.write_gml(g, "communities/community_graph.gml")


print("Graph ready")

#first compute the best partition
partition = community.best_partition(g)

#drawing

plt.figure(figsize=(50,50))
plt.title("eTurismo", fontsize=48)


size = float(len(set(partition.values())))
pos = nx.spring_layout(g)
count = 0

labels = dict()
for node in g:
    if len(g[node]) >= 3:
        labels[node] = node


f = open("Top10NodesPerCommunity(eTurismo).txt", "w")

for com in set(partition.values()) :

    list_nodes = [nodes for nodes in partition.keys()
                                if partition[nodes] == com]



    # Rank nodes by degrees
    ranked_nodes = g.degree(nbunch=list_nodes)
    # Sort the nodes by relevance
    nodes_by_rank = sorted(ranked_nodes, key=ranked_nodes.get, reverse=True)
    # Top 10 relevant nodes
    relevant_nodes = nodes_by_rank[:10]

    most_relevant_node = nodes_by_rank[0]

    # Store the community graph
    c_graph = g.subgraph(list_nodes)
    c_graph.name = most_relevant_node
    nx.write_gml(c_graph, "communities/c{}.gml".format(count))

    f.write("Community {}:\n".format(count))
    f.write("\t" + ", ".join(relevant_nodes) + "\n\n")

    color = (random(), random(), random())
    nx.draw_networkx_nodes(g, pos, list_nodes, node_size = [len(g[node])*100 for node in list_nodes], node_color = color, font_size=8)
    count = count + 1


f.close()

nx.draw_networkx_edges(g,pos, alpha=0.5)
nx.draw_networkx_labels(g,pos, labels)
# plt.show()
plt.savefig("community(eTurismo).pdf", dpi=1000, facecolor='w', edgecolor='w', orientation='portrait', papertype=None, format=None,
                transparent=False, bbox_inches=None, pad_inches=0.1)