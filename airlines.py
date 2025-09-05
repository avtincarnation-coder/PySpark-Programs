from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("airlines").getOrCreate()

data = [(1, "Indigo" , "India" , "Bhutan" ),
(2, "Air Asia" , "Aus" , "India" ),
(3, "Indigo" , "Bhutan" , "Nepal" ),
(4, "Spice jet" , "SriLanka" , "Bhutan" ),
(5, "Indigo" , "Nepal" , "SriLanka" ),
(6, "Air Asia" , "India" , "Japan" ),
(7, "Spice jet" , "Bhutan" , "Nepal" )]

df = spark.createDataFrame(data,["id","airway","source","destination"])

df_source_flag = df.select("id","airway","source")
df_dest_flag = df.select("id","airway","destination")
df_nomatch_source = df_source_flag.join(df_dest_flag, (df_source_flag["source"]==df_dest_flag["destination"]) &
                                   (df_source_flag["airway"]==df_dest_flag["airway"]), "left_anti")

df_nomatch_dest = df_dest_flag.join(df_source_flag, (df_source_flag["source"]==df_dest_flag["destination"]) &
                                   (df_source_flag["airway"]==df_dest_flag["airway"]), "left_anti")

df_flight_join = df_nomatch_source.join(df_nomatch_dest, on = "airway", how= "inner")

df_final_result = df_flight_join.select("airway","source","destination")

df_final_result.show(truncate=False)







