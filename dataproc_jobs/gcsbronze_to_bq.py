from pyspark.sql import SparkSession
from pyspark.sql.functions import col,when

spark=SparkSession.builder \
					.appName("GCSBronzeToBQ") \
					.getOrCreate()

df=spark.read.parquet("gs://insurance-28022026/bronze_parquet/eid_structured")

# Handling NULLs
for c in df.columns:
    df=df.withColumn(c, when(col(c)=="NULL",None).otherwise(col(c)))

# Data type casting
df=df.withColumn("age",col("age").cast("int")) \
        .withColumn("premium",col("premium").cast("double")) \
        .withColumn("claim_amount", col("claim_amount").cast("double")) \
        .withColumn("coverage_amount",col("coverage_amount").cast("double"))

# Duplicates Handling
df=df.dropDuplicates()

# Write to BigQuery
df.write.format("bigquery") \
        .option("table","retail-proj-1.bronze_insu.eid_healthcare") \
        .mode("overwrite") \
        .save()