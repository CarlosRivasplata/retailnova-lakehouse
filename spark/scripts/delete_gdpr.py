from pyspark.sql import SparkSession
from delta.tables import DeltaTable
from delta import configure_spark_with_delta_pip

builder = SparkSession.builder \
    .appName("delete-gdpr") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate()

try:
    tabla = DeltaTable.forPath(spark, "/opt/data/silver/ventas_clean")
    tabla.delete("id_cliente = 'C001'")
    print("Operación GDPR ejecutada con éxito.")
except Exception as e:
    print(f"Error al ejecutar borrado GDPR: {e}")

spark.stop()
