from pyspark.sql import SparkSession, functions as F
from delta import configure_spark_with_delta_pip

builder = SparkSession.builder \
    .appName("bronze-to-silver") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate()

# Carga de Bronze
df = spark.read.format("delta").load("/opt/data/bronze/ventas_delta")

# Transformaciones de negocio
df = df.withColumn("fecha", F.to_date("fecha","yyyy-MM-dd"))

# Separación en Cuarentena (Registros con valores imposibles)
df_invalid = df.filter((F.col("unidades") <= 0) | (F.col("precio_total") <= 0) | (F.col("id_venta").isNull()))
df_valid = df.filter((F.col("unidades") > 0) & (F.col("precio_total") > 0) & (F.col("id_venta").isNotNull())).dropDuplicates(["id_venta"])

# Escritura en Silver Clean
(df_valid.write.format("delta")
 .mode("overwrite")
 .partitionBy("fecha")
 .option("mergeSchema", "true")
 .save("/opt/data/silver/ventas_clean"))

# Escritura en Silver Quarantine
(df_invalid.write.format("delta")
 .mode("overwrite")
 .option("mergeSchema", "true")
 .save("/opt/data/silver/ventas_cuarentena"))

spark.stop()
