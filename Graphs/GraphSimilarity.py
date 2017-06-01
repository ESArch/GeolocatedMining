import networkx as nx
import numpy as np
import math
from scipy.spatial import distance
import matplotlib.pyplot as plt

def select_k(spectrum, minimum_energy = 0.9):
    running_total = 0.0
    total = sum(spectrum)
    if total == 0.0:
        return len(spectrum)
    for i in range(len(spectrum)):
        running_total += spectrum[i]
        if running_total / total >= minimum_energy:
            return i + 1
    return len(spectrum)




def laplacian_similarity(g1, g2):
    laplacian1 = nx.spectrum.laplacian_spectrum(g1)
    laplacian2 = nx.spectrum.laplacian_spectrum(g2)

    k1 = select_k(laplacian1)
    k2 = select_k(laplacian2)
    k = min(k1, k2)

    similarity = sum((laplacian1[:k] - laplacian2[:k]) ** 2)

    return similarity


def compute_beliefs(g, nodes, prior_beliefs):
    num_nodes = len(nodes)
    degrees = g.degree()

    one_norm = 1. / (2 + 2 * max(degrees.values()))

    c1 = 2 + sum(degrees.values())
    c2 = sum([x * x - 1 for x in degrees.values()])
    frobenius_norm = np.sqrt((-c1 + np.sqrt(c1 * c1 + 4 * c2)) / (8 * c2))

    if math.isnan(one_norm):
        one_norm = 0.
    if math.isnan(frobenius_norm):
        frobenius_norm = 0.

    h = np.maximum(one_norm, frobenius_norm)


    a = (4. * np.power(h, 2)) / (1. - 4. * np.power(h, 2))
    c = (2. * h) / (1. - 4. * np.power(h, 2))

    A = nx.adjacency_matrix(g, nodes)

    D = np.zeros((num_nodes, num_nodes))
    for i in range(num_nodes):
        # D[i][i] = np.sum(A[i,:])
        D[i][i] = degrees[nodes[i]]



    matrix = np.eye(num_nodes) + a * D - c * A

    final_beliefs = np.linalg.solve(matrix, prior_beliefs)

    return final_beliefs


def compute_inverse_beliefs(g, nodes):
    num_nodes = len(nodes)
    degrees = g.degree()

    # print(degrees)
    one_norm = 1. / (2 + 2 * max(degrees.values()))

    c1 = 2 + sum(degrees.values())
    c2 = sum([x * x - 1 for x in degrees.values()])
    frobenius_norm = np.sqrt((-c1 + np.sqrt(c1 * c1 + 4 * c2)) / (8 * c2))

    if math.isnan(one_norm):
        one_norm = 0.
    if math.isnan(frobenius_norm):
        frobenius_norm = 0.

    h = np.maximum(one_norm, frobenius_norm)
    # print(one_norm, frobenius_norm, h)


    a = (4. * np.power(h, 2)) / (1. - 4. * np.power(h, 2))
    c = (2. * h) / (1. - 4. * np.power(h, 2))

    prior_beliefs = np.zeros((num_nodes, 1)) + 0.5

    D = np.zeros((num_nodes, num_nodes))
    for i in range(num_nodes):
        D[i][i] = degrees[nodes[i]]

    A = nx.adjacency_matrix(g, nodes)
    # print(A)

    # print(np.eye(num_nodes))
    # print(a*D)
    # print(c*A.todense())

    matrix = np.eye(num_nodes) + a * D - c * A.todense()

    inv_matrix = np.linalg.inv(matrix)

    return inv_matrix


def FaBP_similarity(g1, g2):

    my_g1 = g1.copy()
    my_g2 = g2.copy()

    g1_nodes = my_g1.nodes()
    g2_nodes = my_g2.nodes()

    nodes_not_in_g1 = list(set(g2_nodes) - set(g1_nodes))
    nodes_not_in_g2 = list(set(g1_nodes) - set(g2_nodes))
    all_nodes = list(set(g1_nodes).union(set(g2_nodes)))

    num_activated_nodes = math.ceil(len(all_nodes)*0.1)
    # activated_nodes = np.random.choice(range(len(all_nodes)), num_activated_nodes)
    prior_beliefs = np.zeros((len(all_nodes), 1)) + 0.5
    # for i in activated_nodes:
    #     prior_beliefs[i,0] += 0.01

    clique = nx.compose(my_g1, my_g2)
    # for node in clique:
    #     for edge in clique[node]:
    #         if clique[node][edge]['weight'] != 0:
    #             clique[node][edge]['weight']=100

    # clique = nx.complete_graph(len(all_nodes))
    # clique = nx.Graph()
    # clique.add_nodes_from(range(len(all_nodes)))

    my_g1.add_nodes_from(nodes_not_in_g1)
    my_g2.add_nodes_from(nodes_not_in_g2)

    # g1_inv_beliefs = compute_inverse_beliefs(my_g1, all_nodes)
    # g2_inv_beliefs = compute_inverse_beliefs(my_g2, all_nodes)
    # clique_inv_beliefs = compute_inverse_beliefs(clique, clique.nodes())
    g1_final_beliefs = compute_beliefs(my_g1, all_nodes, prior_beliefs)
    g2_final_beliefs = compute_beliefs(my_g2, all_nodes, prior_beliefs)
    clique_final_beliefs = compute_beliefs(clique, all_nodes, prior_beliefs)


    # print(np.matmul(g1_inv_beliefs, prior_beliefs))
    # print(np.matmul(g2_inv_beliefs, prior_beliefs))


    # similarity = list()
    # for i in range(len(all_nodes)):
    #     similarity.append( 1 / (1 + distance.euclidean(g1_inv_beliefs[:,i], g2_inv_beliefs[:,i])) )



    # dist = list()
    # for i in range(len(all_nodes)):
    #     dist.append(distance.euclidean(g1_inv_beliefs[:, i], g2_inv_beliefs[:, i]))
    #
    # max_d = max(dist)
    # if max_d == 0:
    #     similarity = 1.
    # else:
    #     similarity = [ 1 - np.sqrt((x/max_d)) for x in dist]

    # similarity = list()
    # for i in range(len(all_nodes)):
    #     dist = 0
    #     for j in range(len(all_nodes)):
    #         dist += np.power(g1_inv_beliefs[j,i] - g2_inv_beliefs[j,i], 2) / np.power(clique_inv_beliefs[j,i] - 0.5, 2)
    #     similarity.append(1 / (1 + np.sqrt(dist)))

    # similarity = list()
    # for i in range(len(all_nodes)):
    #     beliefs_g1 = g1_inv_beliefs[:,i]
    #     beliefs_g2 = g2_inv_beliefs[:,i]
    #     beliefs_difference = beliefs_g1 - beliefs_g2
    #
    #     numerator = np.power(beliefs_difference, 2)
    #     denominator = np.power(clique_inv_beliefs[:,i] - 0.5, 2)
    #     distance = np.sqrt(sum(np.divide(numerator, denominator)))
    #     similarity.append(1 / (1 + distance))


    # final_beliefs_g1 = np.matmul(g1_inv_beliefs, prior_beliefs)
    # final_beliefs_g2 = np.matmul(g2_inv_beliefs, prior_beliefs)
    # final_beliefs_clique = np.matmul(clique_inv_beliefs, prior_beliefs)

    distance = 0.
    for i in range(len(all_nodes)):
        distance += np.power(g1_final_beliefs[i,0]-g2_final_beliefs[i,0], 2) / np.power(clique_final_beliefs[i,0]-prior_beliefs[i,0], 2)

    distance = np.sqrt(distance)

    # print(distance)
    similarity = 1 / (1+distance)




    # print(np.average(similarity))

    # return  np.average(similarity)
    return  similarity




#
# g1 = nx.read_gml("../eTurismo/graphs/g0.gml")
# g2 = nx.read_gml("../eTurismo/graphs/g1.gml")
#
# g1 = nx.read_gml("../MovieLens/graphs/g0.gml")
# g2 = nx.read_gml("../MovieLens/graphs/g1.gml")
#
# print(FaBP_similarity(g1,g2))


K5 = nx.complete_graph(5)
eK5 = nx.complete_graph(5)
eK5.remove_edge(4,3)


C5 = nx.cycle_graph(5)
eC5 = nx.cycle_graph(5)
eC5.remove_edge(1,2)

P5 = nx.path_graph(5)
eP5 = nx.path_graph(5)
eP5.remove_edge(2,3)
# #
print(FaBP_similarity(K5, eK5))
print(FaBP_similarity(P5, eP5))
print(FaBP_similarity(C5, eC5))

# nx.draw(nx.compose(K5, eK5))
# plt.show()





