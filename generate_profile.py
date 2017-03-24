import json
import os

import time


env_python = "/home/loki/miniconda3/envs/spark-lang/bin/python3"
text_path = '/home/loki/Datasets/dewiki.xml'
out_path = 'de.json'
# env_python = "/home/andriy/anaconda3/envs/spark/bin/python3"
# text_path = '/home/andriy/Datasets/de/de_1000000'
# text_path = '/home/andriy/Code/PyCharmProjects/lang_ident/requirements.txt'

os.environ.setdefault('PYSPARK_PYTHON', env_python)
# os.environ.setdefault('PYSPARK_DRIVER_PYTHON', "/home/andriy/anaconda3/envs/spark/bin/ipython3")

from pyspark.sql import SparkSession

spark = SparkSession.builder. \
    master("spark://192.168.1.46:7077"). \
    appName("lang_profile_generation"). \
    getOrCreate()

# spark = SparkSession.builder. \
#     master("spark://XPS-13-9360:7077"). \
#     appName("ProofOfConcept"). \
#     getOrCreate()

sc = spark.sparkContext

from sklearn import feature_extraction

f = feature_extraction.text.CountVectorizer()
anal = f.build_analyzer()


def word2grams(start=int, end=int, word=str):
    li = list()
    for n in range(start, end):
        l = [word[i:i + n] for i in range(len(word) - n + 1)]
        if l:
            li.extend(l)
    return li


# def mapper(line):
#     lang_profile = defaultdict(int)
#     tokens = anal(line)
#     for token in tokens:
#         grams = word2grams(1, 10, token)
#         for gram in grams:
#             lang_profile[gram] += 1
#     return dict(lang_profile)

# def mapper(line):
#     tokens = anal(line)
#     for token in tokens:
#         grams = word2grams(1, 10, token)
#         return grams


def tokenizer(line):
    tokens = anal(line)
    return tokens


def n_gramizer(word):
    return word2grams(1, 10, word)


# def mapper(line):
#     lang_profile = defaultdict(int)
#     count_grams = defaultdict(int)
#     tokens = anal(line)
#     for token in tokens:
#         grams = word2grams(1, 10, token)
#         for gram in grams:
#             lang_profile[gram] += 1
#             count_grams[len(gram)] += 1
#     return dict(lang_profile), dict(count_grams)


# def reducer(line1, line2):
#     dic11 = line1[0]
#     dic12 = line1[1]
#     dic21 = line2[0]
#     dic22 = line2[1]
#     from collections import Counter
#     dic11 = Counter(dic11)
#     dic21 = Counter(dic21)
#     res1 = dic11 + dic21
#     dic12 = Counter(dic12)
#     dic22 = Counter(dic22)
#     res2 = dic12 + dic22
#     return res1, res2

# def reducer(line1: dict, line2: dict):
#     from collections import Counter
#     line1 = Counter(line1)
#     line2 = Counter(line2)
#     res = line1 + line2
#     return dict(res)

# with open(text_path) as fh:
#     text = fh.read()
fh = open(text_path)

textFile = sc.parallelize(fh)
# textFile = sc.textFile(text_path)
# textFile.map(lambda line: len(line.split())).reduce(lambda a, b: a if (a > b) else b)
# res = textFile.flatMap(lambda line: line.split()).map(lambda word: (word, 1)).reduceByKey(lambda a, b: a + b)
# res = textFile.map(lambda line: mapper(line)).reduce(lambda tuple_one, tuple_two: reducer(tuple_one, tuple_two))
# res = textFile.flatMap(lambda line: mapper(line)).map(lambda word: (word, 1))
t1 = time.time()
res = textFile.flatMap(lambda line: tokenizer(line)) \
    .flatMap(lambda word: n_gramizer(word)) \
    .map(lambda gram: (gram, 1)) \
    .reduceByKey(lambda a, b: a + b)
# print(dict(res.collect()))

with open(out_path, 'w') as fp:
    json.dump(dict(res.collect()), fp)

t2 = time.time() - t1
print(t2)
# print(res)

# print(res)

# PYSPARK_PYTHON="/home/andriy/anaconda3/envs/spark/bin/python3"
# os.environ['PYSPARK_PYTHON'] = PYSPARK_PYTHON
