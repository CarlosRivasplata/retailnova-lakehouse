from pyspark.sql import SparkSession, functions as F
from delta import configure_spark_with_delta_pip

builder = SparkSession.builder \
    .appName("silver-to-gold") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate()

df = spark.read.format("delta").load("/opt/data/silver/ventas_clean")

gold = (df.groupBy("fecha","id_tienda","canal")
          .agg(F.sum("precio_total").alias("importe_total"),
               F.sum("unidades").alias("unidades_totales")))

(gold.write.format("delta")
 .mode("overwrite")
 .partitionBy("fecha")
 .option("mergeSchema", "true")
 .save("/opt/data/gold/ventas_diarias_por_tienda"))

spark.stop()
