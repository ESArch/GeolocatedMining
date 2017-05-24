import networkx as nx
import matplotlib.pyplot as plt

num_graphs = 58

for i in range(num_graphs):
    filename = "graphs/g{}.gml".format(i)
    filename2 = "img/g{}.pdf".format(i)
    g = nx.read_gml(filename)

    # print(nx.info(g))


    labels = dict()
    for node in g:
        if len(g[node]) >= 5:
            labels[node] = node


    # sub = g.subgraph(sub_nodes)

    d = nx.degree(g)


    pos = nx.spring_layout(g)  # G is my graph
    plt.figure(i, figsize=(30,30))
    plt.title(g.name, fontsize=32)
    nx.draw(g, pos, nodelist=d.keys(), node_size=[v*100 for v in d.values()], node_color='#A0CBE2', edge_color='#BB0000', width=1, edge_cmap=plt.cm.Blues, with_labels=False, font_size=8)
    nx.draw_networkx_labels(g, pos, labels)
    plt.savefig(filename2, dpi=1000, facecolor='w', edgecolor='w', orientation='portrait', papertype=None, format=None,
                transparent=False, bbox_inches=None, pad_inches=0.1)
