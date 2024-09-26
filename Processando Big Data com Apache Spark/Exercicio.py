from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()
    
df = spark.read.json("C:/Users/lucas/OneDrive/Pós_Graduação/9 - Linguagens de programação para ciência de dados (Python com Spark)/Tema 06 - Processando Big Data com Apache Spark/people.json") 

df.show()

df.printSchema()

df.select("name").show()

df.select(df['name'], df['age'] + 1).show()

df.groupBy("age").count().show()


df.createOrReplaceTempView("people")

sqlDF = spark.sql("SELECT * FROM people")
sqlDF.show()

df.createGlobalTempView("people")

spark.sql("SELECT * FROM global_temp.people").show()

spark.newSession().sql("SELECT * FROM global_temp.people").show()