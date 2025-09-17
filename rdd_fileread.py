from pyspark import SparkContext

sc = SparkContext("local","file read")

rdd = sc.parallelize("D:/java/data_new/employees.csv")

rdd_filter = rdd.filter(lambda x : x in 'j')

print(rdd_filter.collect())

#print(rdd.take(5))