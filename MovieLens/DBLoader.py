import psycopg2
import csv

def insert(query, input):
    con = None

    try:
        con = psycopg2.connect(database='MovieLens', user='postgres', password='postgres', host='localhost')

        cur = con.cursor()

        cur.executemany(query, input)
        con.commit()


    except psycopg2.DatabaseError as e:
        if con:
            con.rollback()
        print("Error {}".format(e))

    finally:

        if con:
            con.close()


def insertGTag():

    query = "INSERT INTO gtag(gtag_id, gtag_tag) VALUES(%s, %s)"

    gtags = list()
    with open("/home/dieaigar/TFMDiego/ml-20m/genome-tags.csv") as f:
        next(f)
        for line in f:
            splitted_line = line.strip().split(",")
            id = int(splitted_line[0])
            tag = splitted_line[1]
            gtags.append((id, tag))

    insert(query, gtags)


def insertUser():

    query = "INSERT INTO users(user_id) VALUES(%s)"

    users_set = set()
    users = list()

    with open("/home/dieaigar/TFMDiego/ml-20m/ratings.csv") as f:
        next(f)
        for line in f:
            splitted_line = line.strip().split(",")
            id = int(splitted_line[0])
            users_set.add(id)

    for user in users_set:
        users.append((user,))

    print(users)

    insert(query, users)


def insertMovie():

    query = "INSERT INTO movie(movie_id, movie_title) VALUES(%s, %s)"

    movies = list()

    with open("/home/dieaigar/TFMDiego/ml-20m/movies.csv") as f:
        next(f)
        for line in f:
            splitted_line = line.strip().split(",")
            id = int(splitted_line[0])
            title = splitted_line[1]
            movies.append((id, title))

    insert(query, movies)


def insertGenre():

    query = "INSERT INTO genre(genre_id, genre_name) VALUES(%s, %s)"

    genres_set = set()
    genres = list()

    with open("/home/dieaigar/TFMDiego/ml-20m/movies.csv", newline='') as f:
        reader = csv.reader(f, delimiter=',')
        next(reader)
        for line in reader:
            movie_genres = line[2].split('|')

            for genre in movie_genres:
                if genre == '(no genres listed)':
                    continue
                genres_set.add(genre)

    id = 0
    for genre in genres_set:
        genres.append((id, genre))
        id += 1


    insert(query, genres)


def insertGenre():
    query = "INSERT INTO genre(genre_id, genre_name) VALUES(%s, %s)"

    genres_set = set()
    genres = list()

    with open("/home/dieaigar/TFMDiego/ml-20m/movies.csv", newline='') as f:
        reader = csv.reader(f, delimiter=',')
        next(reader)
        for line in reader:
            movie_genres = line[2].split('|')

            for genre in movie_genres:
                if genre == '(no genres listed)':
                    continue
                genres_set.add(genre)

    id = 0
    for genre in genres_set:
        genres.append((id, genre))
        id += 1

    insert(query, genres)


