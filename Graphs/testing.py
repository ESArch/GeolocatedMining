import networkx as nx
import matplotlib.pyplot as plt

G = nx.Graph()
G.add_edge("Af fasdfa fasdfasd ", 2)

nx.draw(G,node_size=500, with_labels=True)

plt.savefig('temp.pdf')
plt.show()
