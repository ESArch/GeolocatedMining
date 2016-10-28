from pymongo import MongoClient
from nltk.corpus import stopwords
from nltk import bigrams
import string
import re
import operator
from collections import Counter
from collections import defaultdict

client = MongoClient('localhost', 27017)
db = client['twitter_db']
collection = db['valencia_collection']

emoticons_str = r"""
    (?:
        [:=;] # Eyes
        [oO\-]? # Nose (optional)
        [D\)\]\(\]/\\OpP] # Mouth
    )"""

regex_str = [
    emoticons_str,
    r'<[^>]+>', # HTML tags
    r'(?:@[\w_]+)', # @-mentions
    r"(?:\#+[\w_]+[\w\'_\-]*[\w_]+)", # hash-tags
    r'http[s]?://(?:[a-z]|[0-9]|[$-_@.&amp;+]|[!*\(\),]|(?:%[0-9a-f][0-9a-f]))+', # URLs

    r'(?:(?:\d+,?)+(?:\.?\d+)?)', # numbers
    r"(?:[a-z][a-z'\-_]+[a-z])", # words with - and '
    r'(?:[\w_]+)', # other words
    r'(?:\S)' # anything else
    # Faltan palabras con tildes y algunos problemas con la Ñ
]

tokens_re = re.compile(r'('+'|'.join(regex_str)+')', re.VERBOSE | re.IGNORECASE)
emoticon_re = re.compile(r'^'+emoticons_str+'$', re.VERBOSE | re.IGNORECASE)

def tokenize(s):
    return tokens_re.findall(s)

def preprocess(s, lowercase=False):
    tokens = tokenize(s)
    if lowercase:
        tokens = [token if emoticon_re.search(token) else token.lower() for token in tokens]
    return tokens


# Punctuation signs
punctuation = list(string.punctuation)+[ '¿', '¡', '…']
# stopwords + punctuation signs
stop = stopwords.words('spanish') + [w.title() for w in stopwords.words('spanish')] + [w.upper() for w in stopwords.words('spanish')] + [w.lower() for w in stopwords.words('spanish')] + punctuation


tweets = collection.find({'lang' : 'es'})


def mostFrequentTerms():
    # Punctuation signs
    punctuation = list(string.punctuation) + ['¿', '¡', '…']
    # stopwords + punctuation signs
    stop = stopwords.words('spanish') + [w.title() for w in stopwords.words('spanish')]\
           + [w.upper() for w in stopwords.words('spanish')] + [w.lower() for w in stopwords.words('spanish')] + punctuation

    # Most frequent terms
    count_all = Counter()

    for tweet in tweets:
        terms_stop = [term for term in preprocess(tweet['text']) if term not in stop]
        count_all.update(terms_stop)

    print(count_all.most_common(10))


    # Bigramas
    terms_bigram = bigrams(terms_stop)
    count_bigrams = Counter()
    count_bigrams.update(terms_bigram)

    print(count_bigrams.most_common(10))



tweets = collection.find()
com = defaultdict(lambda  : defaultdict(int))

for tweet in tweets:
    terms_only = [term for term in preprocess(tweet['text'])
                  if term not in stop
                  and not term.startswith(('#', '@'))]

    # Build co-occurrence matrix
    for i in range(len(terms_only)-1):
        for j in range(i+1, len(terms_only)):
            w1, w2 = sorted([terms_only[i], terms_only[j]])
            if w1 != w2:
                com[w1][w2] += 1

com_max = []
# For each term, look for the most common co-occurrence
for t1 in com:
    t1_max_terms = sorted(com[t1].items(), key = operator.itemgetter(1), reverse = True)[:5]
    for t2, t2_count in t1_max_terms:
        com_max.append(((t1,t2), t2_count))
# Get the most frequent co-occurrences
terms_max = sorted(com_max, key = operator.itemgetter(1), reverse = True)
print(terms_max[:5])





