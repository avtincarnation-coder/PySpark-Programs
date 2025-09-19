#Testing-Addded

from pyspark import SparkContext

sc = SparkContext("local","RDD Example")

data = [1,2,3,4,5]

rdd = sc.parallelize(data)

print(rdd.collect())

