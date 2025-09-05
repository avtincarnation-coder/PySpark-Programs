from pyspark.sql import SparkSession
from pyspark.sql import Window
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("stocks").getOrCreate()

data = [("2023-01-01","Infosys",1400),
("2023-01-02","Infosys",None),
("2023-01-03","Infosys",1450),
("2023-01-04","Infosys",None),
("2023-01-05","Infosys",None),
("2023-01-05","Infosys",None),
("2023-01-01","Relaince",2300),
("2023-01-02","Relaince",None)]

df = spark.createDataFrame(data,["datakey","stocks","Price"])

window_spec = Window.partitionBy("stocks").orderBy("datakey")

derived_df = df.withColumn("derived_stock",coalesce(lag("Price",1).over(window_spec),"Price",lag("Price",2).over(window_spec),lag("Price",3).over(window_spec)))

derived_df.show()





