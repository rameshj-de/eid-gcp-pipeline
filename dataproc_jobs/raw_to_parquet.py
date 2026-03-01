from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_extract

spark = SparkSession.builder.appName("RawToBronze").getOrCreate()

df_raw = spark.read.text("gs://insurance-28022026/raw-data/eid_healthcare_1gb.txt")

df_parsed = df_raw.select(
    regexp_extract("value", r"^(.*?)%", 1).alias("name"),
    regexp_extract("value", r"%(.*?)@", 1).alias("age"),
    regexp_extract("value", r"@(.*?)\|", 1).alias("location"),
    regexp_extract("value", r"\|(.*?)#", 1).alias("premium"),
    regexp_extract("value", r"#(.*?)&", 1).alias("status"),
    regexp_extract("value", r"&(.*?)\^", 1).alias("policy_type"),
    regexp_extract("value", r"\^(.*?)~", 1).alias("agent_id"),
    regexp_extract("value", r"~(.*?)!", 1).alias("email"),
    regexp_extract("value", r"!(.*?)\$", 1).alias("phone"),
    regexp_extract("value", r"\$(.*?)\*", 1).alias("join_date"),
    regexp_extract("value", r"\*(.*?)\?", 1).alias("region"),
    regexp_extract("value", r"\?(.*?)=", 1).alias("hospital_code"),
    regexp_extract("value", r"=(.*?)\+", 1).alias("claim_amount"),
    regexp_extract("value", r"\+(.*?)>", 1).alias("disease"),
    regexp_extract("value", r">(.*)$", 1).alias("coverage_amount")
)

df_parsed.write.mode("overwrite") \
    .parquet("gs://insurance-28022026/bronze_parquet/eid_structured/")

spark.stop()