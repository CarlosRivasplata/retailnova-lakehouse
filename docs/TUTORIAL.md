# Tutorial de Desarrollo para el Alumno

Este tutorial está dirigido a los estudiantes que deseen desplegar, explorar y extender el **RetailNova Lakehouse** en un entorno local.  Siguiendo estos pasos, podrás comprender la arquitectura, ejecutar el pipeline y realizar tus propias ampliaciones.

---

## 1. Preparación del Entorno

1. **Instalar Docker Desktop**: descarga e instala Docker Desktop para tu sistema operativo (macOS, Windows o Linux).  
2. **Clonar el repositorio**: abre una terminal y ejecuta:

   ```bash
   git clone https://github.com/CarlosRivasplata/retailnova-lakehouse.git
   cd retailnova-lakehouse
   ```

3. **Configurar recursos**: asigna al menos 4 GB de RAM y 2 vCPUs en la configuración de Docker Desktop para asegurar un funcionamiento fluido del stack.

---

## 2. Arranque y Despliegue

1. Ejecuta el siguiente comando para levantar el entorno de contenedores y bootstrapear las dependencias:

   ```bash
   docker compose up -d
   ```

2. Espera unos minutos hasta que todos los servicios estén levantados.  Puedes verificar el estado en la interfaz de Docker Desktop o ejecutando `docker compose ps`.

---

## 3. Uso de Apache Airflow

1. Abre tu navegador y navega a [http://localhost:8080](http://localhost:8080).  
2. Inicia sesión con las credenciales por defecto (`airflow` / `airflow`).  
3. Activa el DAG `retailnova_lakehouse_pipeline` y haz clic en **Trigger** para ejecutar el pipeline.  
4. Observa el gráfico del DAG, los logs de cada tarea y los intentos de re‑ejecución si se produce alguna falla.

---

## 4. Inspección de Datos y Resultados

1. **Datos en Delta Lake**: las tablas en formato Delta se almacenan en la carpeta `data/`.  Examina la estructura de las carpetas `bronze/`, `silver/` y `gold/`.  Para explorar los datos puedes usar PySpark interactivo:

   ```bash
   docker exec -it spark-delta bash
   pyspark --packages io.delta:delta-core_2.12:2.4.0
   ```

2. **Reportes de Calidad**: después de ejecutar el DAG, abre el archivo `data/gx/uncommitted/data_docs/local_site/index.html` en tu navegador para visualizar el dashboard de Great Expectations.

3. **Historias de Time Travel**: utiliza el comando descrito en el README para inspeccionar la historia de versiones de la tabla `ventas_clean`.

---

## 5. Ejercicios Propuestos

1. **Agregar un nuevo campo**: modifica el archivo `data/raw/ventas_raw.csv` para incluir una nueva columna (por ejemplo, `descuento`).  Ajusta los scripts para que soporten la evolución de esquema y valida que la columna se incorpora automáticamente a la tabla Silver y luego a Gold.  
2. **Crear una nueva regla de calidad**: añade una expectativa en `quality_check_silver.py` para validar que `precio_unitario = precio_total / unidades`.  Ejecuta el pipeline y observa si la nueva regla se refleja en los Data Docs.  
3. **Añadir un KPI**: amplía `silver_to_gold.py` para calcular el margen de beneficio medio por día (`ingresos - costes`) y particionar la tabla Gold por `fecha` y `tienda`.  
4. **Integrar una nueva fuente**: simula la ingestión de datos de tráfico o meteorología; modifica `bronze_load.py` para fusionar estas fuentes y analiza cómo afecta a la arquitectura del Lakehouse.

---

## 6. Buenas Prácticas

- Mantén tu entorno limpio ejecutando `docker compose down` cuando finalices las pruebas.  
- Utiliza ramas (`git checkout -b feature/tu-feature`) para experimentar y no mezclar cambios en la rama principal.  
- Documenta cualquier modificación que realices en los archivos Markdown correspondientes.  
- Si necesitas deshacer cambios en la tabla Delta, usa la funcionalidad de Time Travel (`RESTORE TABLE ... TO VERSION AS OF ...`).

---

*Este tutorial está pensado para acompañar tu aprendizaje en Big Data y arquitectura de datos moderna.  Experimenta con el Lakehouse, amplía las reglas de negocio y explora nuevas fuentes de datos para comprender cómo los datos transforman la logística y el retail.*