from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

# Configuración profesional: configure_spark_with_delta_pip descarga los JARs necesarios
builder = SparkSession.builder \
    .appName("bronze-load") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate()

# Lectura de datos crudos
df = spark.read.option("header", "true").option("inferSchema", "true").csv("/opt/input/ventas_raw.csv")

# Escritura en Bronze con Schema Evolution habilitado
(df.write.format("delta")
 .mode("overwrite")
 .option("mergeSchema", "true")
 .save("/opt/data/bronze/ventas_delta"))

spark.stop()
