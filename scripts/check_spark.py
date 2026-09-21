from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("resolve-kafka-connector").getOrCreate()
assert spark.range(3).count() == 3
spark.stop()
