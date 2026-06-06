import great_expectations as gx
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
import sys
import os

# 1. Configuración de Spark Profesional
builder = SparkSession.builder \
    .appName("quality-check-silver-pro") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate()

# 2. Configuración del Contexto de GX
gx_root_dir = "/opt/data/gx"
if not os.path.exists(gx_root_dir):
    os.makedirs(gx_root_dir)

context = gx.get_context(project_root_dir=gx_root_dir)

# 3. Configuración de Data Source y Asset
data_source_name = "retailnova_spark_source"
data_asset_name = "silver_clean_asset"
df = spark.read.format("delta").load("/opt/data/silver/ventas_clean")

try:
    data_source = context.data_sources.get(data_source_name)
except:
    data_source = context.data_sources.add_spark(name=data_source_name)

try:
    data_asset = data_source.get_asset(data_asset_name)
except:
    data_asset = data_source.add_dataframe_asset(name=data_asset_name)

batch_def_name = "batch_full_silver"
try:
    batch_definition = data_asset.get_batch_definition(batch_def_name)
except:
    batch_definition = data_asset.add_batch_definition_whole_dataframe(name=batch_def_name)

# 4. Obtener o crear la Expectation Suite
suite_name = "retailnova_quality_suite"
try:
    suite = context.suites.get(suite_name)
    print(f"Suite '{suite_name}' encontrada. Actualizando reglas...")
except:
    suite = context.suites.add(gx.ExpectationSuite(name=suite_name))
    print(f"Suite '{suite_name}' creada.")

# --- DEFINICIÓN DE REGLAS (Nombres estándar GX 1.x) ---
expectations = [
    # 1. id_venta no puede ser nulo
    gx.expectations.ExpectColumnValuesToNotBeNull(column="id_venta"),
    
    # 2. id_venta debe ser único
    gx.expectations.ExpectColumnValuesToBeUnique(column="id_venta"),
    
    # 3. unidades debe ser mayor o igual a 1 (Usamos Between con min_value)
    gx.expectations.ExpectColumnValuesToBeBetween(column="unidades", min_value=1),
    
    # 4. precio_total debe ser mayor que 0 (Usamos Between con un valor muy pequeño como mínimo)
    gx.expectations.ExpectColumnValuesToBeBetween(column="precio_total", min_value=0.01),
    
    # 5. canal solamente puede contener: TIENDA o ECOMMERCE
    gx.expectations.ExpectColumnValuesToBeInSet(column="canal", value_set=["TIENDA", "ECOMMERCE"]),
    
    # 6. fecha no puede ser nula
    gx.expectations.ExpectColumnValuesToNotBeNull(column="fecha"),
    
    # 7. id_cliente no puede ser nulo
    gx.expectations.ExpectColumnValuesToNotBeNull(column="id_cliente"),
    
    # 8. producto no puede ser nulo
    gx.expectations.ExpectColumnValuesToNotBeNull(column="producto")
]

# Limpiar expectativas previas y añadir las nuevas
for exp in expectations:
    suite.add_expectation(exp)

# 5. Validación y Checkpoint
val_def_name = "validation_silver"
try:
    validation_definition = context.validation_definitions.get(val_def_name)
except:
    validation_definition = context.validation_definitions.add(
        gx.ValidationDefinition(name=val_def_name, data=batch_definition, suite=suite)
    )

checkpoint_name = "checkpoint_retailnova"
try:
    checkpoint = context.checkpoints.get(checkpoint_name)
except:
    checkpoint = context.checkpoints.add(
        gx.Checkpoint(
            name=checkpoint_name,
            validation_definitions=[validation_definition],
            result_format="SUMMARY"
        )
    )

# 6. Ejecución
print("Ejecutando Validación Certificada...")
checkpoint_result = checkpoint.run(batch_parameters={"dataframe": df})

# 7. Generación de Data Docs
context.build_data_docs()
print(f"SUCCESS: Dashboard de calidad actualizado.")

# 8. Gatekeeper logic
if not checkpoint_result.success:
    print("CRITICAL: Fallo en el contrato de calidad.")
    spark.stop()
    sys.exit(1)
else:
    print("CERTIFIED: 8 reglas validadas con éxito.")
    spark.stop()
