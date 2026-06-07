# Mejoras Técnicas Implementadas - RetailNova Lakehouse

Este documento detalla la evolución técnica del proyecto RetailNova Lakehouse, destacando las mejoras implementadas para transformar una prueba de concepto funcional en una arquitectura de datos de grado empresarial. Cada mejora ha sido diseñada para optimizar el rendimiento, la fiabilidad, la gobernanza y la observabilidad del pipeline.

---

## 1. Configuración Profesional de la Sesión de Spark

**Descripción:** Se ha estandarizado la inicialización de la `SparkSession` en todos los scripts de PySpark para incluir explícitamente las extensiones de Delta Lake y su catálogo. Además, se utiliza `configure_spark_with_delta_pip` para la gestión dinámica de dependencias.

**Detalle Técnico:**
- **`spark.sql.extensions`**: Habilita comandos SQL específicos de Delta Lake (MERGE, UPDATE, DELETE) que son cruciales para operaciones ACID y GDPR.
- **`spark.sql.catalog.spark_catalog`**: Define a Delta Lake como el gestor de tablas por defecto, permitiendo a Spark interactuar nativamente con las tablas Delta.
- **`configure_spark_with_delta_pip`**: Esta función de la librería `delta` descarga e inyecta automáticamente los JARs de Delta Lake en el classpath de Java de Spark. Esto elimina la necesidad de gestionar manualmente las dependencias de Maven, asegurando la compatibilidad y simplificando el despliegue.

**Impacto:** Garantiza la integridad de las operaciones ACID, mejora la compatibilidad y simplifica la gestión de dependencias, haciendo el pipeline más robusto y portable.

---

## 2. Implementación de Particionamiento (Partitioning)

**Descripción:** Se ha aplicado una estrategia de particionamiento por la columna `fecha` en las capas Silver y Gold del Lakehouse.

**Detalle Técnico:**
- Al escribir datos en Delta Lake, se utiliza `.partitionBy("fecha")`. Esto organiza físicamente los datos en el almacenamiento subyacente (ej. `data/silver/ventas_clean/fecha=2023-01-01/`).
- Spark aprovecha esta estructura para el "Data Skipping", leyendo solo los directorios relevantes para una consulta.

**Impacto:** Optimiza drásticamente el rendimiento de las consultas analíticas y de BI, especialmente aquellas que filtran por rangos de tiempo. Reduce el I/O y el tiempo de procesamiento, lo que se traduce en menores costes operativos en entornos de nube.

---

## 3. Evolución del Esquema (Schema Evolution)

**Descripción:** Se ha habilitado la opción `mergeSchema` en las operaciones de escritura de Delta Lake.

**Detalle Técnico:**
- Al utilizar `.option("mergeSchema", "true")` en las escrituras, Delta Lake puede detectar automáticamente cambios en el esquema de los datos entrantes (ej. nuevas columnas) y fusionarlos con el esquema existente de la tabla.

**Impacto:** Proporciona resiliencia al pipeline. El sistema se adapta automáticamente a cambios en las fuentes de datos (ej. un ERP añade una nueva columna), evitando fallos del pipeline y reduciendo la necesidad de intervención manual.

---

## 4. Orquestación Avanzada con Apache Airflow

**Descripción:** Se ha implementado Apache Airflow como el orquestador central del pipeline, utilizando una configuración robusta basada en Docker Compose.

**Detalle Técnico:**
- **Arquitectura DRY (Don't Repeat Yourself):** El `docker-compose.yml` utiliza YAML Anchors (`&airflow-common`) para centralizar la configuración de los servicios de Airflow (Webserver, Scheduler), mejorando la mantenibilidad y reduciendo errores.
- **Comunicación Inter-Contenedor:** Se mapeó el socket de Docker (`/var/run/docker.sock`) al contenedor de Airflow, permitiendo que Airflow ejecute comandos (`docker exec`) en el contenedor de Spark.
- **Idempotencia y Resiliencia:** La configuración de Airflow y los scripts están diseñados para soportar múltiples re-ejecuciones sin fallos de infraestructura.
- **Auto-Bootstrap de Dependencias:** Los contenedores de Airflow instalan automáticamente las librerías necesarias (`delta-spark`, `great-expectations`) al iniciar, garantizando un entorno "Plug & Play".

**Impacto:** Automatización completa del flujo de datos, monitoreo visual a través de la interfaz web, gestión de reintentos y una base sólida para la escalabilidad y fiabilidad del pipeline en producción.

---

## 5. Calidad de Datos Empresarial con Great Expectations 1.x (Gatekeeper)

**Descripción:** Se ha integrado Great Expectations (GX) como un "Gatekeeper" de calidad, utilizando su Fluent API 1.x para validar los datos en la capa Silver antes de su promoción a Gold.

**Detalle Técnico:**
- **Contrato de Datos de 8 Reglas:** Se definieron expectativas explícitas para asegurar la integridad, unicidad y consistencia de los datos de ventas (ej. `id_venta` no nulo y único, `unidades` >= 1, `canal` en `['TIENDA', 'ECOMMERCE']`).
- **Lógica de Idempotencia en GX:** El script de calidad utiliza patrones `get_or_create` para Data Sources, Assets, Suites y Checkpoints. Esto permite que el script se ejecute múltiples veces sin fallar por objetos GX ya existentes.
- **Actualización Dinámica de Expectation Suite:** La suite de expectativas se actualiza en cada ejecución, asegurando que cualquier cambio en las reglas de negocio en el código se refleje inmediatamente en las validaciones.
- **Generación de Data Docs (HTML):** GX genera automáticamente un dashboard interactivo en HTML (`data/gx/uncommitted/data_docs/local_site/index.html`) que visualiza el estado de la calidad de los datos, ideal para stakeholders no técnicos.

**Impacto:** Garantiza la fiabilidad de los datos consumidos por el negocio, previene la propagación de datos erróneos a la capa Gold y proporciona un mecanismo de auditoría visual y técnica de la calidad del dato.

---

## 6. Gobernanza Avanzada: Delta Lake Time Travel

**Descripción:** Se ha aprovechado la capacidad inherente de Delta Lake para mantener un historial completo de todas las transacciones realizadas sobre las tablas.

**Detalle Técnico:**
- Cada operación (escritura, borrado, actualización) en una tabla Delta crea una nueva versión inmutable.
- Esta historia es accesible programáticamente (ej. `DeltaTable.history().show()`) y permite consultar versiones anteriores de la tabla.

**Impacto:** Fundamental para la auditoría, el cumplimiento normativo (GDPR), la recuperación ante desastres (rollback a una versión anterior) y el análisis de linaje de datos.

---

## 7. Comparativa de Evolución del Proyecto

La siguiente tabla resume la transformación del proyecto desde su concepción inicial hasta su estado actual de arquitectura empresarial:

| Característica | Estado Inicial (Concepto) | Estado Actual (Empresarial) | Valor Añadido |
| :--- | :--- | :--- | :--- |
| **Configuración Spark** | Genérica, sin Delta JARs | Profesional (`configure_spark_with_delta_pip`) | Compatibilidad Delta garantizada, gestión automática de JARs. |
| **Particionamiento** | No implementado | Por `fecha` en Silver y Gold | Optimización de consultas, reducción de I/O, escalabilidad. |
| **Evolución Esquema** | No gestionado | `mergeSchema` habilitado | Resiliencia ante cambios de esquema, menos fallos de pipeline. |
| **Orquestación** | Scripts Python independientes | **Apache Airflow** (DRY, Docker Socket) | Automatización, monitoreo visual, reintentos, estabilidad. |
| **Calidad de Datos** | Filtros básicos (`df.filter`) | **Great Expectations 1.x** (8 reglas, Gatekeeper, Idempotente) | Contrato de datos explícito, reportes visuales, detención proactiva de errores. |
| **Gestión Dependencias** | Instalación manual (`pip install`) | **Auto-Bootstrap** (`pip3` en `docker-compose`) | Entorno "Plug & Play", reduce errores humanos, despliegue rápido. |
| **Gobernanza** | Borrado GDPR básico | **Delta Time Travel** (Auditoría completa) | Trazabilidad de cambios, cumplimiento legal, recuperación de versiones. |
| **Resiliencia** | Fallos por re-ejecución | **Idempotencia** en GX y scripts | Re-ejecuciones seguras, estabilidad del pipeline. |
| **Observabilidad** | Logs de consola | **Airflow UI + GX Data Docs (HTML)** | Monitoreo técnico y dashboards de calidad para negocio. |

---
**Autor:** Carlos Alberto Rivasplata Guerrero  
**Especialista en Big Data & Business Intelligence**
