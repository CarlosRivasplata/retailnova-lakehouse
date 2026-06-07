# Documentación de Scripts del Proyecto RetailNova

Este documento explica la funcionalidad de cada script de PySpark y del DAG de Airflow que componen el pipeline del **RetailNova Lakehouse**.  La intención es que cualquier desarrollador o analista pueda comprender rápidamente qué hace cada componente, cuáles son sus entradas y salidas, y cómo se interconectan dentro del flujo de datos.

---

## Scripts de Spark

### bronze_load.py
- **Propósito:** cargar el fichero de ventas crudo (`ventas_raw.csv`) y almacenarlo en formato Delta en la capa Bronze.  
- **Entrada:** `data/raw/ventas_raw.csv` (o la ruta configurada).  
- **Salida:** `data/bronze/ventas_delta`.  
- **Detalles:** inicializa una `SparkSession` con soporte Delta, lee el CSV con inferencia de esquema, añade la columna `ingestion_timestamp` y escribe la tabla con modo `overwrite` para que el pipeline sea idempotente.

### bronze_to_silver.py
- **Propósito:** limpiar y normalizar la tabla Bronze para crear la capa Silver.  
- **Entrada:** tabla Delta Bronze (`data/bronze/ventas_delta`).  
- **Salidas:** `data/silver/ventas_clean` (datos válidos) y `data/silver/ventas_cuarentena` (registros inválidos).  
- **Detalles:** valida cada registro (ej. que `unidades ≥ 1`, `precio_total > 0`, `canal` pertenece al catálogo de canales); convierte `fecha` a tipo `date`; elimina duplicados (`id_venta`); y escribe las tablas particionadas por `fecha` para optimizar lecturas futuras.

### quality_check_silver.py
- **Propósito:** validar la calidad de la capa Silver mediante **Great Expectations 1.x** y actuar como *gatekeeper* antes de promover los datos a Gold.  
- **Entrada:** `data/silver/ventas_clean`.  
- **Salidas:** reporte JSON de validación y Data Docs HTML (`data/gx/uncommitted/data_docs/local_site/index.html`).  
- **Detalles:** inicializa el contexto de GX, define un datasource Spark, crea un asset y una Expectation Suite con ocho reglas; utiliza lógica idempotente para no duplicar objetos; ejecuta un checkpoint; escribe los Data Docs y devuelve un código de retorno que indica si la validación fue exitosa.  Si la validación falla, el script termina con error y Airflow detiene el DAG.

### silver_to_gold.py
- **Propósito:** agregar los datos limpios de la capa Silver para producir métricas de negocio en la capa Gold.  
- **Entrada:** `data/silver/ventas_clean`.  
- **Salida:** `data/gold/ventas_diarias_por_tienda`, particionada por `fecha`.  
- **Detalles:** agrupa por `fecha`, `tienda` y `canal`; calcula la suma de `unidades` y `precio_total`; escribe en Delta con `.partitionBy("fecha")`; y conserva la semántica ACID de Delta Lake.

### delete_gdpr.py
- **Propósito:** eliminar registros específicos en la capa Silver para cumplir con el GDPR u otros requisitos de negocio.  
- **Entrada:** `data/silver/ventas_clean`.  
- **Detalles:** crea un objeto `DeltaTable` de la tabla Silver y ejecuta `delete` con un predicado (por ejemplo, `id_cliente = 'C001'`).  Cada eliminación se registra en el historial de Delta Lake para facilitar su auditoría y recuperación.

---

## DAG de Airflow: retailnova_pipeline.py

El archivo `dags/retailnova_pipeline.py` define el DAG `retailnova_lakehouse_pipeline` que orquesta la ejecución de todos los scripts anteriores en un orden secuencial.  

### Tareas

1. **load_bronze**: ejecuta `bronze_load.py` dentro del contenedor `spark-delta` utilizando `docker exec`.  
2. **transform_silver**: ejecuta `bronze_to_silver.py`.  
3. **quality_check_gx**: ejecuta `quality_check_silver.py` y, basado en su resultado, decide si continuar o detener el pipeline.  
4. **aggregate_gold**: ejecuta `silver_to_gold.py`.  
5. **cleanup_gdpr**: ejecuta `delete_gdpr.py` para realizar borrados lógicos de datos sensibles.  

### Flujo y Dependencias

Las tareas se enlazan usando el operador `>>`, asegurando que la salida de una se utilice como entrada de la siguiente:

```python
t1 = load_bronze
t2 = transform_silver
t3 = quality_check_gx
t4 = aggregate_gold
t5 = cleanup_gdpr

t1 >> t2 >> t3 >> t4 >> t5
```

Airflow gestiona automáticamente los reintentos según la configuración del DAG (retries=1, retry_delay de 5 minutos) y registra los logs de cada tarea.  Si la validación de GX falla, la tarea `quality_check_gx` retorna un código de error y el DAG se marca como fallido.

---

## Referencias

- [README.md](../README.md) – Guía general del proyecto y contexto logístico.  
- [MEJORAS_TECNICAS.md](MEJORAS_TECNICAS.md) – Explicación de las optimizaciones que afectan a estos scripts.  
- [DESAFIOS_Y_SOLUCIONES.md](DESAFIOS_Y_SOLUCIONES.md) – Análisis de los desafíos que motivaron estas implementaciones.  
- **PySpark Documentation**: [https://spark.apache.org/docs/latest/api/python/](https://spark.apache.org/docs/latest/api/python/)  
- **Great Expectations Docs**: [https://docs.greatexpectations.io/](https://docs.greatexpectations.io/)

---

*Este documento complementa la documentación principal del proyecto y ayuda a comprender el papel de cada componente en el flujo de datos.*