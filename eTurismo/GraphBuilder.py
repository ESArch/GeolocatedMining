import networkx as nx
import psycopg2
import pandas as pd

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


def select_preferences_by_place(code_place):
    query = "SELECT id_preferences, value_interese FROM place_preferences WHERE code_place = '{}'".format(code_place)
    #print(query)
    result = select(query)
    return [(int(x[0]), preferences[int(x[0])], float(x[1])/5) for x in result]

def select_places_by_preference(preference_id):
    query = "SELECT code_place, value_interese FROM place_preferences WHERE id_preferences = {}".format(preference_id)
    #print(query)
    result = select(query)
    return [(int(x[0]), places[int(x[0])], float(x[1])/5) for x in result]


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


def build_graphs(distance, input_file, output_folder):

    count = 0
    with open("data/{}".format(input_file), "r") as f:
        for line in f:
            node_list = list()
            edge_list = list()
            places_in_pattern = list() # Pattern description

            pattern = line.strip().split(" ")
            for element in pattern:
                first_node = (int(element), places[(int(element))])
                places_in_pattern += [places[(int(element))]]
                nodes = set()
                nodes.add(first_node)
                # nodes, edges = build_graph_lite(first_node, "place", nodes, set(), distance)
                nodes, edges = build_graph_fast(first_node, "place", nodes, set(), distance)

                # Keep only the names
                node_list += [x[1] for x in nodes]
                edge_list += [(x[0][1], x[1][1], x[2]) for x in edges]


            # Build the networkx.Graph
            g = nx.Graph()
            g.add_nodes_from(node_list)
            g.add_weighted_edges_from(edge_list)
            g.name = ", ".join(places_in_pattern)

            print(nx.info(g))

            g_filename = "{}/g{}.gml".format(output_folder,count)
            nx.write_gml(g, g_filename)

            count += 1


def build_graph_lite(node, node_type, nodes, edges, distance ):

    if distance == 0:
        return nodes, edges


    if node_type == "place":

        place_id = node[0]

        found_nodes = select_preferences_by_place(place_id)
        found_nodes = set(found_nodes)
        new_nodes = found_nodes.difference(nodes)

        # print("Found {} tags for movie {}".format(len(new_nodes), movies[movie_id]))

        edges.update([(node, (new_node[0], new_node[1]), new_node[2]) for new_node in new_nodes])
        nodes.update([(new_node[0], new_node[1]) for new_node in new_nodes])

        for new_node in new_nodes:
            nodes, edges = build_graph_lite(new_node, "preference", nodes, edges, distance - 1)



    elif node_type == "preference":
        preference_id = node[0]

        found_nodes = select_places_by_preference(preference_id)
        found_nodes = set(found_nodes)
        new_nodes = found_nodes.difference(nodes)

        # print("Found {} movies for tag {}".format(len(new_nodes), gtags[gtag_id]))

        edges.update([(node, (new_node[0], new_node[1]), new_node[2]) for new_node in new_nodes])
        nodes.update([(new_node[0], new_node[1]) for new_node in new_nodes])
        for new_node in new_nodes:
            nodes, edges = build_graph_lite(new_node, "place", nodes, edges, distance - 1)


    return nodes, edges



def build_graph_fast(node, node_type, nodes, edges, distance):

    if distance == 0:
        return nodes, edges


    if node_type == "place":

        place_id = node[0]

        try:
            found_preferences = place_preferences[:, place_id].to_dict().items()
        except:
            found_preferences = []


        found_nodes = [(x[0], preferences[x[0]], x[1]) for x in found_preferences]
        found_nodes = set(found_nodes)
        new_nodes = found_nodes.difference(nodes)

        # print("Found {} tags for movie {}".format(len(new_nodes), movies[movie_id]))

        edges.update([(node, (new_node[0], new_node[1]), new_node[2]) for new_node in new_nodes])
        nodes.update([(new_node[0], new_node[1]) for new_node in new_nodes])

        for new_node in new_nodes:
            nodes, edges = build_graph_fast(new_node, "preference", nodes, edges, distance - 1)



    elif node_type == "preference":
        preference_id = node[0]

        try:
            found_places = place_preferences[preference_id, :].to_dict().items()
        except:
            found_places = []

        found_nodes = [(x[0], places[x[0]], x[1]) for x in found_places]
        found_nodes = set(found_nodes)
        new_nodes = found_nodes.difference(nodes)

        # print("Found {} movies for tag {}".format(len(new_nodes), gtags[gtag_id]))

        edges.update([(node, (new_node[0], new_node[1]), new_node[2]) for new_node in new_nodes])
        nodes.update([(new_node[0], new_node[1]) for new_node in new_nodes])
        for new_node in new_nodes:
            nodes, edges = build_graph_fast(new_node, "place", nodes, edges, distance - 1)


    return nodes, edges


places = build_place_dict()
preferences = build_preference_dict()
place_preferences = build_place_preferences_table()


# build_graphs(4, "decoded_patterns.txt", "graphs")
build_graphs(4, "places.txt", "place_graphs")


