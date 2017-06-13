from sklearn.cluster import KMeans
import numpy as np
import psycopg2


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

    for entry in result:
        id = entry[0]
        title = entry[1]
        movies[id] = title

    return movies



def find_best_k():
    pattern_contexts = np.load("data/pattern_contexts.npy")

    best_k = 2
    best_intertia = float("inf")

    for i in range(2, 100):
        kmeans = KMeans(n_clusters=i, random_state=0).fit(pattern_contexts)
        print(i, kmeans.inertia_)

        if kmeans.inertia_ < best_intertia:
            best_intertia = kmeans.inertia_
            best_k = i


    print("Best K = {}".format(best_k))


def cluster():
    pattern_contexts = np.load("data/pattern_contexts.npy")
    movies = build_movie_dict()

    with open("data/decoded_patterns.txt", "r") as f:
        patterns = f.readlines()

    k = 35
    kmeans = KMeans(n_clusters=k, random_state=0).fit(pattern_contexts)

    partitions = dict()
    labels = kmeans.labels_
    for i in range(len(labels)):
        partition = partitions.get(labels[i], [])
        partition += [i]
        partitions[labels[i]] = partition



    with open("Kmeans.txt", "w", encoding="utf8") as f:
        count = 1
        for k,v in partitions.items():
            f.write("PARTITION {}\n".format(count))
            for index in v:
                pattern = patterns[index].strip().split(" ")
                pattern_movies = list()
                for item in pattern:
                    pattern_movies.append(movies[int(item)])

                f.write("\t" + " | ".join(pattern_movies) + "\n")

            count += 1
            f.write("\n")


cluster()