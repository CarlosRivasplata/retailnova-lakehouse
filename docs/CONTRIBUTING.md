# Guía de Contribución

Gracias por tu interés en contribuir al proyecto **RetailNova Lakehouse**.  Este documento define las normas de colaboración para mantener la calidad y coherencia del código y la documentación.  Como especialista en Big Data, queremos asegurar que cualquier aporte esté alineado con los objetivos del proyecto y respete el estilo de codificación y documentación.

---

## Requisitos Previos

- Conocimientos de **Python**, **PySpark** y **Delta Lake**.  
- Familiaridad con **Docker** y **Docker Compose**.  
- Manejo básico de **Git** y tener una cuenta en GitHub.  
- Conocimiento del contexto logístico y de negocio de RetailNova para comprender las motivaciones de las mejoras.

---

## Cómo clonar y ejecutar localmente

```bash
git clone https://github.com/CarlosRivasplata/retailnova-lakehouse.git
cd retailnova-lakehouse
docker compose up -d
```

Accede a la interfaz de Airflow en `http://localhost:8080` para activar y disparar el DAG.

---

## Flujo de trabajo para contribuir

1. **Crear una rama** a partir de `main` o `develop` con un nombre descriptivo:

   ```bash
   git checkout -b feature/mi-mejora
   ```

2. **Realizar cambios** en tu rama.  Utiliza comentarios en español claros y sigue el estilo y nomenclatura existentes.  
3. **Actualizar la documentación**.  Si tu mejora afecta a scripts, diagramas o arquitectura, actualiza los archivos `.md` dentro de `docs/` o el `README.md` según corresponda.  
4. **Probar el pipeline** ejecutando `docker compose up` y verificando que el DAG se ejecuta sin errores y que las validaciones de GX son exitosas.  
5. **Cometer y subir** tu rama a GitHub:

   ```bash
   git add .
   git commit -m "Descripción clara de la mejora"
   git push origin feature/mi-mejora
   ```

6. **Crear un Pull Request** desde tu rama hacia `develop` o `main`.  Describe claramente los cambios realizados, el problema que solucionan, el impacto esperado y cualquier información relevante (por ejemplo, enlaces a documentación o discusiones previas).  
7. Un miembro del equipo revisará tu aporte, propondrá ajustes si es necesario y lo fusionará cuando esté listo.

---

## Estilo de código y documentación

- Sigue **PEP 8** para código Python.  Usa herramientas como `black` para formatear automáticamente.  
- Nombres de variables y funciones deben ser descriptivos y en español, salvo términos técnicos que deban mantenerse en inglés por coherencia con la API.  
- Mantén un formato homogéneo en las tablas, listas y títulos de los documentos Markdown.  
- Incluye comentarios y explicaciones en español para facilitar la comprensión a otros colaboradores.  
- Añade siempre las referencias a nuevas imágenes (`images/`) y explicaciones de los diagramas en los archivos que correspondan.

---

## Código de conducta

Este proyecto promueve un entorno abierto y respetuoso.  Esperamos que todos los colaboradores:

- Traten a los demás con respeto y profesionalismo.  
- Eviten lenguaje discriminatorio o despectivo.  
- Valoren la diversidad de opiniones y experiencias.  
- Fomenten la inclusión y la colaboración constructiva.  
- Den crédito a las fuentes y respeten las licencias de terceros.

---

*Siguiendo estas directrices contribuirás a que el proyecto RetailNova Lakehouse crezca con la calidad y profesionalismo que demanda la ingeniería de datos moderna.*