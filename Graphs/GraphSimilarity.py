import networkx as nx
import numpy as np
from scipy.spatial import distance

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


def compute_beliefs(g):
    nodes = g.nodes()
    num_nodes = len(nodes)
    degrees = g.degree()

    one_norm = 1. / (2 + 2 * max(degrees.values()))

    c1 = 2 + sum(degrees.values())
    c2 = sum([x * x for x in degrees.values()])
    frobenius_norm = np.sqrt((-c1 + np.sqrt(c1 * c1 + 4 * c2)) / (8 * c2))

    h = np.maximum(one_norm, frobenius_norm)
    a = (4. * np.power(h, 2)) / (1. - 4. * np.power(h, 2))
    c = (2. * np.power(h, 2)) / (1. - 4. * np.power(h, 2))

    prior_beliefs = np.zeros((num_nodes, 1)) + 0.5

    D = np.zeros((num_nodes, num_nodes))
    for i in range(num_nodes):
        D[i][i] = degrees[nodes[i]]

    A = nx.adjacency_matrix(g, nodes)

    matrix = np.eye(num_nodes) + a * D - c * A

    final_beliefs = np.linalg.solve(matrix, prior_beliefs)

    return final_beliefs


def compute_inverse_beliefs(g, nodes):
    num_nodes = len(nodes)
    degrees = g.degree()

    # print(degrees)
    one_norm = 1. / (2 + 2 * max(degrees.values()))

    c1 = 2 + sum(degrees.values())
    c2 = sum([x * x for x in degrees.values()])
    frobenius_norm = np.sqrt((-c1 + np.sqrt(c1 * c1 + 4 * c2)) / (8 * c2))

    h = np.maximum(one_norm, frobenius_norm)
    # print(one_norm, frobenius_norm, h)


    a = (4. * np.power(h, 2)) / (1. - 4. * np.power(h, 2))
    c = (2. * np.power(h, 2)) / (1. - 4. * np.power(h, 2))

    prior_beliefs = np.zeros((num_nodes, 1)) + 0.5

    D = np.zeros((num_nodes, num_nodes))
    for i in range(num_nodes):
        D[i][i] = degrees[nodes[i]]

    A = nx.adjacency_matrix(g, nodes)

    matrix = np.eye(num_nodes) + a * D - c * A

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

    clique = nx.complete_graph(len(all_nodes))

    my_g1.add_nodes_from(nodes_not_in_g1)
    my_g2.add_nodes_from(nodes_not_in_g2)

    g1_inv_beliefs = compute_inverse_beliefs(my_g1, all_nodes)
    g2_inv_beliefs = compute_inverse_beliefs(my_g2, all_nodes)
    clique_inv_beliefs = compute_inverse_beliefs(clique, clique.nodes())
    # print(clique_inv_beliefs)



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

    similarity = list()
    for i in range(len(all_nodes)):

        numerator = np.power(g1_inv_beliefs[:,i] - g2_inv_beliefs[:,i], 2)
        denominator = np.power(clique_inv_beliefs[:,i] - 0.5, 2)
        distance = np.sqrt(sum(np.divide(numerator, denominator)))
        similarity.append(1 / (1 + distance))




    # print(np.average(similarity))

    return  np.average(similarity)





# g1 = nx.read_gml("../eTurismo/graphs/g0.gml")
# g2 = nx.read_gml("../eTurismo/graphs/g1.gml")
#
# print(FaBP_similarity(g1,g2))




