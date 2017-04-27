import networkx as nx
import numpy as np
from sortedcontainers import SortedDict
import matplotlib.pyplot as plt


def compute_P():
    D = np.zeros((rows, rows))
    for i in range(rows):
        D[i][i] = np.sum(A[i])

    D_inv = np.linalg.inv(D)

    P = D_inv * A

    return P

def compute_PI():

    try:
        P = np.load("P.npy")
    except:
        P = compute_P()
        np.save("P.npy", P)

    PI = c * (1 - c) * P
    for i in range(2, l+1):
        PI += c * np.power((1 - c),i) * np.linalg.matrix_power(P, i)

    return PI

def initialization():
    heaps = list()
    subgraphs = list()
    epsilons = list()
    for i in range(rows):
        subgraph = set()
        subgraph.add(i)
        subgraphs.append(subgraph)
        heap = SortedDict()
        for neighbour in g[nodes[i]].keys():
            idx_n = nodes.index(neighbour)
            heap[idx_n] = PI[i, idx_n]

        heaps.append(heap)
        epsilons.append(np.max(PI[i]) * bratio)

    return subgraphs, heaps, epsilons

def expanding_step(subgraphs, heaps, epsilons):
    count = 0
    while True:
        print("Iteration {}\n".format(count))
        print("Expanding step...\n")
        for i in range(len(subgraphs)):
            # print("\t Expanding graph {}\n".format(i))
            gk = subgraphs[i]
            if len(gk) == 0:
                continue
            vm = heaps[i].peekitem()[0]

            I_V_gk = list(gk)
            if np.max(PI[I_V_gk, vm]) > epsilons[i]:
                subgraphs[i].add(vm)
                heaps[i].popitem()
                for vn in g[nodes[vm]].keys():
                    idx_n = nodes.index(vn)
                    if idx_n not in gk and heaps[i].get(idx_n, None) is None:
                        heaps[i][idx_n] = PI[vm, idx_n]
                    elif heaps[i].get(idx_n, None) is not None:
                        heaps[i][idx_n] = PI[vm, idx_n]

        merge = False
        print("Merging step...")
        for i in range(len(subgraphs)):
            # print("\t Merging graph {}".format(i))
            if len(subgraphs[i]) != 0:
                for j in range(i+1, len(subgraphs)):
                    intersection = subgraphs[i].intersection(subgraphs[j])
                    if len(intersection) != 0:
                        merge = True
                        # subgraphs[i] = subgraphs[i].union(subgraphs[j])
                        subgraphs[i] = intersection
                        subgraphs[j] = set()
                        heaps[i].update(heaps[j])
                        # heap_intersection = SortedDict()
                        # for k,v in heaps[i].items():
                        #     if heaps[j].get(k, None) is None:
                        #         heaps[i].pop(k)
                        epsilons[i] = max(epsilons[i], epsilons[j])

        if not merge:
            break
        count += 1

    return subgraphs






filename = "../MovieLens/test.gml"

c = 0.15
l = 3
bratio = 0.2

g = nx.read_gml(filename)

nodes = sorted(g.nodes())
A = nx.adjacency_matrix(g, nodelist=nodes)
rows, columns = A.shape

try:
    PI = np.load("PI.npy")
except:
    PI = compute_PI()
    np.save("PI.npy", PI)

subgraphs, heaps, epsilons = initialization()
subgraphs = expanding_step(subgraphs, heaps, epsilons)

count = 0
for subgraph in subgraphs:
    if len(subgraph) != 0:
        print(subgraph)
        print(len(subgraph))
        labels = [nodes[i] for i in subgraph]
        sub = g.subgraph(labels)
        nx.draw(sub, with_labels=True)
        filename = "sub{}.pdf".format(str(count))
        count += 1
        plt.savefig(filename)
        # plt.show()
