from pyspark.sql.functions import col
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pyspark.sql.types as T
from operator import add
spark = SparkSession.builder.master("local").appName("PySpark_Postgres_test").getOrCreate()
dburl="jdbc:postgresql://ec2-13-40-49-105.eu-west-2.compute.amazonaws.com:5432/testdb"


df = spark.read.format("jdbc").option("url","jdbc:postgresql://ec2-13-40-49-105.eu-west-2.compute.amazonaws.com:5432/testdb").option("driver", "org.postgresql.Driver").option("dbtable", "terralunapp").option("user", "consultants").option("password", "WelcomeItc@2022").load()
print(df.printSchema())

df=df.withColumn("double_amount", df.total_volume*2)

print(df.show(5))

df.write.mode('overwrite') \
    .saveAsTable("pythongroup.terralunapp")