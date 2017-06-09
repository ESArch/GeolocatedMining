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


def build_community_graph(min_relevance):
    movies = build_movie_dict()
    gtags = build_gtag_dict()
    gtag_score = build_gtag_score_table(0.95)

    edges = list()
    for x in gtag_score.to_dict().keys():
        edge = (gtags[x[0]], movies[x[1]])
        edges.append(edge)

    g = nx.Graph()
    g.add_edges_from(edges)

    return g

try:
    g = nx.read_gml("communities/community_graph.gml")
    print("Reading graph...")
except:
    print("Building graph...")
    g = build_community_graph(0.95)
    nx.write_gml(g, "communities/community_graph.gml")


print("Graph ready")

print(nx.info(g))

#first compute the best partition
partition = community.best_partition(g)

#drawing

plt.figure(figsize=(100,100))
plt.title("MovieLens", fontsize=48)


size = float(len(set(partition.values())))
pos = nx.spring_layout(g)
count = 0

labels = dict()
for node in g:
    if len(g[node]) >= 5:
        labels[node] = node


f = open("Top10NodesPerCommunity(MovieLens).txt", "w")

for com in set(partition.values()) :

    list_nodes = [nodes for nodes in partition.keys()
                                if partition[nodes] == com]

    # Rank nodes using Pagerank algorithm
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
plt.savefig("community(MovieLens).pdf", dpi=1000, facecolor='w', edgecolor='w', orientation='portrait', papertype=None, format=None,
                transparent=False, bbox_inches=None, pad_inches=0.1)

