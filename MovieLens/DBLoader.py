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


def update(query):
    con = None

    try:
        con = psycopg2.connect(database='MovieLens', user='postgres', password='postgres', host='localhost')

        cur = con.cursor()

        cur.execute(query)
        con.commit()


    except psycopg2.DatabaseError as e:
        if con:
            con.rollback()
        print("Error {}".format(e))

    finally:

        if con:
            con.close()


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

def fixMovieTitle():

    #with open("/home/dieaigar/TFMDiego/ml-20m/movies.csv") as f:

    with open("C:\\Users\\Arch\\Desktop\\Clase\\TFM\\ml-20m\\movies.csv", encoding='utf8') as f:
        reader = csv.reader(f, delimiter=',')
        next(reader)
        for line in reader:
            id = int(line[0])
            title = str(line[1])
            query = "UPDATE movie SET movie_title = '{}' WHERE movie_id = {}".format(title.replace("'", "''"), id)
            update(query)




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


def insertMovieGenre():
    query = "INSERT INTO movie_genre(movie_id, genre_id) VALUES(%s, %s)"
    selectQuery = "SELECT genre_id, genre_name FROM genre"
    genres_dict = dict()

    result = select(selectQuery)

    for entry in result:
        id = int(entry[0])
        name = entry[1]
        genres_dict[name] = id


    genres = list()

    with open("C:\\Users\\Arch\\Desktop\\Clase\\TFM\\ml-20m\\movies.csv", encoding='utf8') as f:
        reader = csv.reader(f, delimiter=',')
        next(reader)
        for line in reader:
            movie_id = int(line[0])
            movie_genres = line[2].split('|')

            for genre in movie_genres:
                if genre == '(no genres listed)':
                    continue
                genres.append((movie_id, genres_dict[genre]))

    insert(query, genres)


def insertGTagScore():

    query = "INSERT INTO gtag_score(gtag_id, movie_id, gs_relevance) VALUES(%s, %s, %s)"

    gtag_scores = list()

    with open("C:\\Users\\Arch\\Desktop\\Clase\\TFM\\ml-20m\\genome-scores.csv", encoding='utf8') as f:
        reader = csv.reader(f, delimiter=',')
        next(reader)
        for line in reader:
            gtag_id = int(line[1])
            movie_id = int(line[0])
            relevance = float(line[2])

            gtag_scores.append((
                gtag_id,
                movie_id,
                relevance
            ))


    insert(query, gtag_scores)


def insertRating():

    query = "INSERT INTO rating(user_id, movie_id, rating_value) VALUES(%s, %s, %s)"

    ratings = list()
    count = 0

    with open("C:\\Users\\Arch\\Desktop\\Clase\\TFM\\ml-20m\\ratings.csv", encoding='utf8') as f:
        reader = csv.reader(f, delimiter=',')
        next(reader)
        for line in reader:
            user_id = int(line[0])
            movie_id = int(line[1])
            value = float(line[2])

            ratings.append((
                user_id,
                movie_id,
                value
            ))
            count += 1

            if count == 50000:
                insert(query, ratings)
                count = 0
                ratings = list()


    insert(query, ratings)

fixMovieTitle()