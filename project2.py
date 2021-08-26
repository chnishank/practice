from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StringType, IntegerType 
spark=SparkSession.builder.appName('Spark started').getOrCreate()
schema = StructType() \
    .add("ID",IntegerType(),True) \
    .add("Name",StringType(),True) \
    .add("Sex",StringType(),True) \
    .add("Age",IntegerType(),True) \
    .add("Height",IntegerType(),True) \
    .add("Weight",IntegerType(),True)\
    .add("Team",StringType(),True) \
    .add("NOC",StringType(),True) \
    .add("Games",StringType(),True) \
    .add("Year",IntegerType(),True) \
    .add("Season",StringType(),True) \
    .add("City",StringType(),True) \
    .add("Sport",StringType(),True) \
    .add("Event",StringType(),True) \
    .add("Medal",StringType(),True)
df= spark.read.format("csv") \
      .option("header", True) \
      .schema(schema) \
      .load("C:/ajaysir/P2/P2/athlete_events.csv")  
df.printSchema()
df.show(10)
df_rdd = df.rdd
type(df_rdd)
df_rdd.count()