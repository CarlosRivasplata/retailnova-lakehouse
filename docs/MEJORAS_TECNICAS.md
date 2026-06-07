# Mejoras Técnicas Implementadas – RetailNova Lakehouse

Este documento detalla la evolución técnica del proyecto **RetailNova Lakehouse**, destacando las mejoras implementadas para transformar una prueba de concepto en una arquitectura de datos de grado empresarial.  Como especialista en Big Data, cada mejora se contextualiza dentro del negocio logístico de RetailNova y se explica su impacto en la eficiencia y la gobernanza.

---

## Índice
1. [Configuración Profesional de la Sesión de Spark](#1-configuración-profesional-de-la-sesión-de-spark)  
2. [Implementación de Particionamiento (Partitioning)](#2-implementación-de-particionamiento-partitioning)  
3. [Evolución del Esquema (Schema Evolution)](#3-evolución-del-esquema-schema-evolution)  
4. [Orquestación Avanzada con Apache Airflow](#4-orquestación-avanzada-con-apache-airflow)  
5. [Calidad de Datos Empresarial con Great Expectations 1.x (Gatekeeper)](#5-calidad-de-datos-empresarial-con-great-expectations-1x-gatekeeper)  
6. [Gobernanza Avanzada: Delta Lake Time Travel](#6-gobernanza-avanzada-delta-lake-time-travel)  
7. [Comparativa de Evolución del Proyecto](#7-comparativa-de-evolución-del-proyecto)  
8. [Referencias y Enlaces](#8-referencias-y-enlaces)

---

## 1. Configuración Profesional de la Sesión de Spark

**Descripción:** Todos los scripts de PySpark inicializan una `SparkSession` configurada explícitamente para Delta Lake.  Se utilizan las extensiones de Delta y `configure_spark_with_delta_pip` para inyectar automáticamente los JARs de Delta en el entorno de Java de Spark.

**Detalle Técnico:**

- **`spark.sql.extensions`**: habilita comandos SQL específicos de Delta (MERGE, UPDATE, DELETE) necesarios para operaciones ACID y procesos de GDPR.  
- **`spark.sql.catalog.spark_catalog`**: define a Delta Lake como el gestor de tablas por defecto, permitiendo a Spark interactuar de forma nativa con tablas Delta.  
- **`configure_spark_with_delta_pip`**: función de la librería `delta` que descarga e inyecta automáticamente los JARs de Delta Lake; elimina la necesidad de gestionar manualmente dependencias de Maven.

**Impacto:** Se garantiza la integridad de las operaciones ACID, se mejora la compatibilidad y se simplifica la gestión de dependencias.  Esto hace que el pipeline sea más robusto, portable y alineado con los requisitos de resiliencia operativa de RetailNova.

---

## 2. Implementación de Particionamiento (Partitioning)

**Descripción:** Las tablas de las capas Silver y Gold se particionan por la columna `fecha`.  Esta técnica organiza los datos en directorios independientes según su valor de fecha (por ejemplo `data/silver/ventas_clean/fecha=2023-01-01/`).

**Detalle Técnico:**

- Al escribir las tablas Delta se invoca `.partitionBy("fecha")`.  
- Spark utiliza el particionamiento para aplicar **Data Skipping**: sólo lee las carpetas que coinciden con el rango de fechas de la consulta.  
- Las consultas que filtran por periodos (por ejemplo, ventas diarias, mensuales o anuales) se benefician enormemente de esta disposición.

**Impacto:** Esta optimización reduce el tiempo de I/O y mejora el rendimiento de las consultas analíticas y de BI, reduciendo costes operativos en entornos de nube y cumpliendo los requisitos de agilidad del negocio logístico.

---

## 3. Evolución del Esquema (Schema Evolution)

**Descripción:** En las operaciones de escritura se habilita la opción `mergeSchema`, permitiendo que Delta Lake adapte automáticamente el esquema de la tabla a nuevas columnas que aparezcan en los datos entrantes.

**Detalle Técnico:**

- Al escribir con `.option("mergeSchema", "true")`, Spark fusiona los nuevos campos con el esquema existente de la tabla.  
- Esto evita que el pipeline falle cuando las fuentes de datos añaden nuevas columnas (p. ej. un nuevo atributo de cliente).  
- Se mantiene la compatibilidad de los sistemas de análisis que consumen las tablas Delta.

**Impacto:** Brinda resiliencia ante cambios en las fuentes y reduce la necesidad de intervención manual.  RetailNova puede incorporar rápidamente nuevas métricas o campos sin interrumpir sus procesos analíticos.

---

## 4. Orquestación Avanzada con Apache Airflow

**Descripción:** Se utiliza **Apache Airflow** como orquestador central del pipeline.  La infraestructura se despliega con Docker Compose, aplicando patrones DRY y garantizando la comunicación segura entre contenedores.

**Detalle Técnico:**

- **YAML Anchors** (`&airflow-common`): centralizan la configuración de los servicios de Airflow (webserver, scheduler), reduciendo la duplicación y facilitando la administración.  
- **Comunicación Inter-Contenedor**: se mapea el socket de Docker (`/var/run/docker.sock`) al contenedor de Airflow, permitiendo ejecutar scripts de Spark a través de `docker exec spark-delta python3 script.py`.  
- **Idempotencia y Resiliencia**: el DAG está diseñado para soportar re‑ejecuciones sin que se dupliquen registros ni se pierda integridad.  
- **Auto‑Bootstrap de Dependencias**: los contenedores instalan las librerías necesarias (`delta-spark`, `great-expectations`) en su fase de arranque.

**Impacto:** Automatiza la ejecución, ofrece un monitoreo visual a través de la interfaz web y facilita la escalabilidad.  Airflow gestiona reintentos automáticos y proporciona logs detallados para análisis forense.

---

## 5. Calidad de Datos Empresarial con Great Expectations 1.x (Gatekeeper)

**Descripción:** **Great Expectations** (GX) se integra como validador de la capa Silver.  Se utiliza su Fluent API 1.x para definir un contrato de ocho reglas que verifica la integridad, unicidad y consistencia de los datos antes de promoverlos a la capa Gold.

**Detalle Técnico:**

- **Contrato de Datos de 8 Reglas**: incluye expectativas como `id_venta` no nulo y único, `unidades ≥ 1`, `precio_total > 0`, `canal ∈ {TIENDA, ECOMMERCE}`, `fecha` válida, etc.  
- **Lógica Idempotente**: mediante `get_or_create` se recuperan los Data Sources, Assets, Suites y Checkpoints existentes; sólo se crean si no existen.  
- **Actualización Dinámica**: en cada ejecución se añaden o reemplazan las expectativas, asegurando que los cambios de negocio se reflejen en la suite.  
- **Generación de Data Docs**: GX produce reportes HTML interactivos que documentan el cumplimiento de cada regla.

**Impacto:** Introduce un control de calidad corporativo.  Los datos no conformes se envían a la cuarentena y el pipeline se detiene proactivamente si se incumple el contrato.  El reporte HTML facilita la comunicación con analistas y stakeholders.

---

## 6. Gobernanza Avanzada: Delta Lake Time Travel

**Descripción:** Se aprovecha la funcionalidad de **Time Travel** de Delta Lake para mantener un historial completo de todas las transacciones realizadas sobre las tablas.

**Detalle Técnico:**

- Cada operación sobre una tabla Delta crea una versión inmutable.  
- El historial se consulta mediante `DeltaTable.history()` y es accesible desde cualquier script de PySpark.  
- Permite restaurar versiones anteriores (`dt.restoreToVersion(version)`) y ejecutar consultas SQL en un estado específico (`SELECT * FROM table VERSION AS OF 5`).

**Impacto:** Fundamental para la auditoría y la gobernanza.  Facilita el cumplimiento normativo (GDPR), permite el análisis forense de anomalías y soporta la recuperación ante desastres.  Además, incrementa la transparencia ante los auditores internos y externos de RetailNova.

---

## 7. Comparativa de Evolución del Proyecto

La siguiente tabla resume la transformación del proyecto desde su estado inicial (prueba de concepto) hasta la arquitectura empresarial actual.  Cada fila destaca el valor añadido de la mejora:

| Característica | Estado Inicial (Concepto) | Estado Actual (Empresarial) | Valor Añadido |
| :--- | :--- | :--- | :--- |
| **Configuración Spark** | Genérica, sin Delta JARs | Profesional (`configure_spark_with_delta_pip`) | Garantiza compatibilidad Delta y gestiona dependencias de forma automática |
| **Particionamiento** | No implementado | Partición por `fecha` en Silver y Gold | Optimiza consultas y reduce I/O |
| **Evolución del Esquema** | No gestionado | `mergeSchema` habilitado | Adapta el esquema sin intervención manual |
| **Orquestación** | Scripts independientes | **Apache Airflow** con patrones DRY | Automatización, reintentos, observabilidad |
| **Calidad de Datos** | Filtros básicos (`df.filter`) | **Great Expectations 1.x** (8 reglas, idempotente) | Contrato de datos, dashboard de calidad |
| **Gestión de Dependencias** | Instalación manual | **Auto‑Bootstrap** en `docker-compose` | Despliegue plug & play |
| **Gobernanza** | Borrado GDPR básico | **Delta Time Travel** | Auditoría y recuperación de versiones |
| **Resiliencia** | Fallos en re‑ejecuciones | **Idempotencia** en GX y scripts | Estabilidad operativa |
| **Observabilidad** | Logs de consola | **Airflow UI + GX Data Docs** | Monitoreo técnico y reportes para negocio |

---

## 8. Referencias y Enlaces

- **Documentación Oficial de Delta Lake**: [https://docs.delta.io/latest/delta-intro.html](https://docs.delta.io/latest/delta-intro.html)  
- **Apache Spark Documentation**: [https://spark.apache.org/docs/latest/](https://spark.apache.org/docs/latest/)  
- **Great Expectations Docs**: [https://docs.greatexpectations.io/](https://docs.greatexpectations.io/)  
- [README.md](../README.md) – Introducción general y guía de ejecución.  
- [DESAFIOS_Y_SOLUCIONES.md](DESAFIOS_Y_SOLUCIONES.md) – Análisis de desafíos y soluciones técnicas.  
- [SCRIPTS.md](SCRIPTS.md) – Descripción detallada de cada script y del DAG de Airflow.

---

**Autor:** Carlos Alberto Rivasplata Guerrero  
**Especialista en Big Data & Business Intelligence**