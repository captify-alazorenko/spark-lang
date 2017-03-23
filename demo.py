import os

os.environ.setdefault('PYSPARK_PYTHON', "/home/andriy/anaconda3/envs/spark/bin/python3")
# os.environ.setdefault('PYSPARK_DRIVER_PYTHON', "/home/andriy/anaconda3/envs/spark/bin/ipython3")

from pyspark.sql import SparkSession

spark = SparkSession.builder. \
    master("local"). \
    appName("ProofOfConcept"). \
    getOrCreate()

sc = spark.sparkContext

# PYSPARK_PYTHON="/home/andriy/anaconda3/envs/spark/bin/python3"
# os.environ['PYSPARK_PYTHON'] = PYSPARK_PYTHON


dict1 = {
    'ch1': 4,
    'ch2': 4,
    'ch3': 6,
    'ch4': 3,
    'ch5': 6,
    'ch6': 2,
}
dict2 = {
    'ch1': 2,
    'ch2': 6,
    'ch3': 3,
    'ch4': 1,
    'ch5': 9,
    'ch9': 5,
}
dict3 = {
    'ch1': 4,
    'ch2': 3,
    'ch3': 7,
    'ch4': 2,
    'ch5': 3,
    'ch8': 3,
}

dataset = sc.parallelize([dict1, dict2, dict3])

print(dataset.count())


def reducer(line1: dict, line2: dict):
    from collections import Counter
    line1 = Counter(line1)
    line2 = Counter(line2)
    res = line1 + line2
    return dict(res)


result = dataset.reduce(reducer)
print(result)
