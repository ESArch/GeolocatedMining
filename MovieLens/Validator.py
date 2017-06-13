import networkx as nx
import numpy as np
import Graphs.GraphSimilarity as gs
from operator import itemgetter
import scipy
import psycopg2
import pandas as pd


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


def find_users_by_movies(movie1, movie2, min_rating1, min_rating2):

    query = "SELECT r1.user_id \
        FROM rating r1, rating r2 \
        WHERE r1.movie_id = {} AND r1.rating_value >= {} \
        AND r2.movie_id = {} and r2.rating_value >= {} \
        AND r1.user_id = r2.user_id".format(movie1, movie2, min_rating1, min_rating2)

    result = select(query)

    return len(result)


def build_ratings_table(min_rating):
    query = "SELECT user_id, movie_id, rating_value FROM rating WHERE rating_value >= {}".format(min_rating)
    result = select(query)

    tuples = [(x[0], x[1]) for x in result]
    values = [x[2] for x in result]

    index = pd.MultiIndex.from_tuples(tuples, names=['user_id', 'movie_id'])

    s = pd.Series(values, index=index)
    s.sort_index(inplace=True)

    return s



def validate(num_transactions):
    pattern_contexts = np.load("data/pattern_contexts.npy")
    transaction_contexts = np.load("data/transaction_contexts.npy")

    with open("data/decoded_itemsets.txt", "r") as f:
        transactions = f.readlines()


    with open("data/decoded_patterns.txt", "r") as f, open("validation({}).txt".format(num_transactions), "w") as g:
        count = 0
        global_confidences = list()

        seen_ratings = build_ratings_table(0)
        liked_ratings = build_ratings_table(4)

        for line in f:

            context_vector = pattern_contexts[count, :].reshape(1, pattern_contexts.shape[1])

            distances = scipy.spatial.distance.cdist(transaction_contexts, context_vector, 'cosine')
            distances = np.reshape(distances, distances.shape[0])

            most_relevant_transaction_indexes = distances.argsort()[:num_transactions]
            # most_relevant_transaction = transactions[most_relevant_transaction_index].strip().split(" ")
            transaction_items = set()
            for i in most_relevant_transaction_indexes:
                transaction = transactions[i].strip().split(" ")
                for transaction_item in transaction:
                    transaction_items.add(int(transaction_item))

            pattern = line.strip().split(" ")

            local_confidences = list()
            for pattern_item in pattern:

                pattern_item_id = int(pattern_item)
                like_first = set(liked_ratings[:, pattern_item_id].to_dict().keys())
                seen_first = set(seen_ratings[:, pattern_item_id].to_dict().keys())


                for transaction_item in transaction_items:

                    transaction_item_id = int(transaction_item)

                    # like_both = find_users_by_movies(pattern_item_id, 4, transaction_item_id, 4)
                    # seen_both = find_users_by_movies(pattern_item_id, 0, transaction_item_id, 4)

                    like_second = set(liked_ratings[:, transaction_item_id].to_dict().keys())

                    like_both = len(like_second.intersection(like_first))
                    seen_both = len(like_second.intersection(seen_first))


                    if seen_both > 0:
                        local_confidences.append(like_both/seen_both)

            local_confidence = np.average(local_confidences)
            print("Confidence for pattern {}: {}".format(count, local_confidence))
            g.write("Confidence for pattern {}: {}\n".format(count, local_confidence))

            count += 1
            global_confidences.append(local_confidence)

            # if count > 10:
            #     break

        global_confidence = np.average(global_confidences)
        print("Global confidence: {}".format(global_confidence))
        g.write("\nGlobal confidence: {}".format(global_confidence))





def validate2(num_transactions):
    pattern_contexts = np.load("data/pattern_contexts.npy")
    transaction_contexts = np.load("data/transaction_contexts.npy")

    with open("data/decoded_itemsets.txt", "r") as f:
        transactions = f.readlines()

    with open("data/decoded_patterns.txt", "r") as f, open("validation2({}).txt".format(num_transactions),
                                                           "w") as g:
        count = 0
        global_confidences = list()

        seen_ratings = build_ratings_table(0)
        liked_ratings = build_ratings_table(4)

        for line in f:

            context_vector = pattern_contexts[count, :].reshape(1, pattern_contexts.shape[1])

            distances = scipy.spatial.distance.cdist(transaction_contexts, context_vector, 'cosine')
            distances = np.reshape(distances, distances.shape[0])

            most_relevant_transaction_indexes = distances.argsort()[:num_transactions]
            # most_relevant_transaction = transactions[most_relevant_transaction_index].strip().split(" ")
            transaction_items = set()
            for i in most_relevant_transaction_indexes:
                transaction = transactions[i].strip().split(" ")
                for transaction_item in transaction:
                    transaction_items.add(int(transaction_item))

            pattern = line.strip().split(" ")

            local_confidences = list()

            liked_pattern = set(liked_ratings[:, int(pattern[0])].to_dict().keys())
            for pattern_item in pattern:
                pattern_item_id = int(pattern_item)
                liked_pattern = liked_pattern.intersection(
                    set(liked_ratings[:, pattern_item_id].to_dict().keys()))

            for transaction_item in transaction_items:

                transaction_item_id = int(transaction_item)

                # like_both = find_users_by_movies(pattern_item_id, 4, transaction_item_id, 4)
                # seen_both = find_users_by_movies(pattern_item_id, 0, transaction_item_id, 4)

                like_transaction_item = set(liked_ratings[:, transaction_item_id].to_dict().keys())
                seen_transaction_item = set(seen_ratings[:, transaction_item_id].to_dict().keys())

                like_both = len(liked_pattern.intersection(like_transaction_item))
                seen_both = len(liked_pattern.intersection(seen_transaction_item))

                if seen_both > 0:
                    local_confidences.append(like_both / seen_both)

            local_confidence = np.average(local_confidences)
            print("Confidence for pattern {}: {}".format(count, local_confidence))
            g.write("Confidence for pattern {}: {}\n".format(count, local_confidence))

            count += 1
            global_confidences.append(local_confidence)

            # if count > 10:
            #     break

        global_confidence = np.average(global_confidences)
        print("Global confidence: {}".format(global_confidence))
        g.write("\nGlobal confidence: {}".format(global_confidence))



def validate3(num_transactions):
    pattern_contexts = np.load("data/pattern_contexts.npy")
    transaction_contexts = np.load("data/transaction_contexts.npy")

    with open("data/decoded_itemsets.txt", "r") as f:
        transactions = f.readlines()

    with open("data/decoded_patterns.txt", "r") as f, open("validation3({}).txt".format(num_transactions),
                                                           "w") as g:
        count = 0
        global_confidences = list()

        seen_ratings = build_ratings_table(0)
        liked_ratings = build_ratings_table(4)

        for line in f:

            context_vector = pattern_contexts[count, :].reshape(1, pattern_contexts.shape[1])

            distances = scipy.spatial.distance.cdist(transaction_contexts, context_vector, 'cosine')
            distances = np.reshape(distances, distances.shape[0])

            most_relevant_transaction_indexes = distances.argsort()[:num_transactions]
            # most_relevant_transaction = transactions[most_relevant_transaction_index].strip().split(" ")
            transaction_items = set()
            for i in most_relevant_transaction_indexes:
                transaction = transactions[i].strip().split(" ")
                for transaction_item in transaction:
                    transaction_items.add(int(transaction_item))

            pattern = line.strip().split(" ")

            local_confidences = list()

            liked_pattern = set(liked_ratings[:, int(pattern[0])].to_dict().keys())
            for pattern_item in pattern:
                pattern_item_id = int(pattern_item)
                liked_pattern = liked_pattern.intersection(
                    set(liked_ratings[:, pattern_item_id].to_dict().keys()))

            liked = len(liked_pattern)
            for transaction_item in transaction_items:

                transaction_item_id = int(transaction_item)

                # like_both = find_users_by_movies(pattern_item_id, 4, transaction_item_id, 4)
                # seen_both = find_users_by_movies(pattern_item_id, 0, transaction_item_id, 4)

                like_transaction_item = set(liked_ratings[:, transaction_item_id].to_dict().keys())
                seen_transaction_item = set(seen_ratings[:, transaction_item_id].to_dict().keys())

                # like_both = len(liked_pattern.intersection(like_transaction_item))
                seen_both = len(liked_pattern.intersection(seen_transaction_item))

                if liked > 0:
                    local_confidences.append(seen_both / liked)

            local_confidence = np.average(local_confidences)
            print("Confidence for pattern {}: {}".format(count, local_confidence))
            g.write("Confidence for pattern {}: {}\n".format(count, local_confidence))

            count += 1
            global_confidences.append(local_confidence)

            # if count > 10:
            #     break

        global_confidence = np.average(global_confidences)
        print("Global confidence: {}".format(global_confidence))
        g.write("\nGlobal confidence: {}".format(global_confidence))

def validate4(num_transactions):
    pattern_contexts = np.load("data/pattern_contexts.npy")
    transaction_contexts = np.load("data/transaction_contexts.npy")

    with open("data/decoded_itemsets.txt", "r") as f:
        transactions = f.readlines()

    with open("data/decoded_patterns.txt", "r") as f, open("validation4({}).txt".format(num_transactions),
                                                           "w") as g:
        count = 0
        global_confidences = list()

        seen_ratings = build_ratings_table(0)
        liked_ratings = build_ratings_table(4)

        for line in f:

            context_vector = pattern_contexts[count, :].reshape(1, pattern_contexts.shape[1])

            distances = scipy.spatial.distance.cdist(transaction_contexts, context_vector, 'cosine')
            distances = np.reshape(distances, distances.shape[0])

            most_relevant_transaction_indexes = distances.argsort()[:num_transactions]
            pattern = line.strip().split(" ")
            local_confidences = list()

            liked_pattern = set(liked_ratings[:, int(pattern[0])].to_dict().keys())
            for pattern_item in pattern:
                pattern_item_id = int(pattern_item)
                liked_pattern = liked_pattern.intersection(
                    set(liked_ratings[:, pattern_item_id].to_dict().keys()))


            for i in most_relevant_transaction_indexes:
                transaction = transactions[i].strip().split(" ")
                seen_transaction = set(seen_ratings[:, int(transaction[0])].to_dict().keys())
                liked_transaction = set(liked_ratings[:, int(transaction[0])].to_dict().keys())
                for transaction_item in transaction:
                    transaction_item_id = int(transaction_item)
                    seen_transaction = set(seen_ratings[:, transaction_item_id].to_dict().keys())
                    liked_transaction = set(liked_ratings[:, transaction_item_id].to_dict().keys())

                liked_both = len(liked_pattern.intersection(liked_transaction))
                seen_both = len(liked_pattern.intersection(seen_transaction))
                print("{} usuarios han visto la transacción".format(seen_both))


                if seen_both > 0:
                    local_confidences.append(liked_both / seen_both)

            local_confidence = np.average(local_confidences)
            print("Confidence for pattern {}: {}".format(count, local_confidence))
            g.write("Confidence for pattern {}: {}\n".format(count, local_confidence))

            count += 1
            global_confidences.append(local_confidence)

        global_confidence = np.average(global_confidences)
        print("Global confidence: {}".format(global_confidence))
        g.write("\nGlobal confidence: {}".format(global_confidence))

def validate5(num_transactions):
    pattern_contexts = np.load("data/pattern_contexts.npy")
    transaction_contexts = np.load("data/transaction_contexts.npy")

    with open("data/decoded_itemsets.txt", "r") as f:
        transactions = f.readlines()

    with open("data/decoded_patterns.txt", "r") as f, open("validation5({}).txt".format(num_transactions),
                                                           "w") as g:
        count = 0
        global_scores = list()

        seen_ratings = build_ratings_table(0)
        liked_ratings = build_ratings_table(4)

        for line in f:

            context_vector = pattern_contexts[count, :].reshape(1, pattern_contexts.shape[1])

            distances = scipy.spatial.distance.cdist(transaction_contexts, context_vector, 'cosine')
            distances = np.reshape(distances, distances.shape[0])

            most_relevant_transaction_indexes = distances.argsort()[:num_transactions]
            pattern = line.strip().split(" ")


            liked_pattern = set(liked_ratings[:, int(pattern[0])].to_dict().keys())
            for pattern_item in pattern:
                pattern_item_id = int(pattern_item)
                liked_pattern = liked_pattern.intersection(
                    set(liked_ratings[:, pattern_item_id].to_dict().keys()))


            local_scores = list()
            for i in most_relevant_transaction_indexes:
                transaction = transactions[i].strip().split(" ")
                seen_transaction = set(seen_ratings[:, int(transaction[0])].to_dict().keys())
                # liked_transaction = set(liked_ratings[:, int(transaction[0])].to_dict().keys())

                transaction_movies = list()
                for transaction_item in transaction:
                    transaction_item_id = int(transaction_item)
                    transaction_movies.append(transaction_item_id)
                    seen_transaction = set(seen_ratings[:, transaction_item_id].to_dict().keys())

                seen_both = liked_pattern.intersection(seen_transaction)

                if len(transaction_movies) == 1:
                    print("Caso 1 ({} users) calculando...".format(len(seen_both)))
                    transaction_rating = np.average(seen_ratings.loc[seen_both,transaction_movies].values)
                    print("Terminado")
                else:
                    print("Caso 2 ({} users) calculando...".format(len(seen_both)))
                    transaction_ratings = list()
                    for user in seen_both:
                        user_rating_avg = np.average(seen_ratings.loc[user,transaction_movies].values)

                        # idx = pd.IndexSlice
                        # print(seen_ratings.loc[idx[user, transaction_movies], :])
                        transaction_ratings.append(user_rating_avg)

                    transaction_rating = np.average(transaction_ratings)
                    print("Terminado")

                local_scores.append(transaction_rating)


            local_score = np.average(local_scores)
            print("Local score for pattern {}: {}".format(count, local_score))
            g.write("Local score for pattern {}: {}\n".format(count, local_score))

            count += 1
            global_scores.append(local_score)
            print("Global score until now: {}".format(np.average(global_scores)))

        global_score = np.average(global_scores)
        print("Global score: {}".format(global_score))
        g.write("\nGlobal score: {}".format(global_score))


# validate(5)
# validate2(5)
# validate3(5)
# validate4(5)
validate5(1)