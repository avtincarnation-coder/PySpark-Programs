from pyspark import SparkContext

sc = SparkContext("local","rdd operations")

data = [54,32,40,76,81,22,43,47,65,24,17]

rdd = sc.parallelize(data)

rdd_map = rdd.map(lambda x : x*2)

print(rdd_map.take(5))

rdd_filter = rdd.filter(lambda x : x>50)

print(rdd_filter.collect())
