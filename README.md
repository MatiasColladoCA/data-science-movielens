
# Análisis de Datos de Películas con Databricks y Delta Lake

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

Este proyecto demuestra un pipeline de datos ETL (Extract, Transform, Load) completo y robusto, construido con Apache Spark en la plataforma Databricks. Se procesa el famoso dataset de MovieLens para extraer insights valiosos sobre las preferencias cinematográficas, almacenando los resultados en **Delta Lake** para garantizar la fiabilidad, el rendimiento y el versionado de los datos.

## Arquitectura del Proyecto

El flujo de trabajo sigue una arquitectura moderna de medallón (Bronze, Silver, Gold), transformando datos crudos en un activo de alto valor para el análisis.

```mermaid
graph TD
    A[Datos Crudos (Bronze)<br>MovieLens CSVs] --> B{Databricks & Apache Spark<br>(Proceso ETL)};
    B --> C[Transformación y Limpieza<br>(Silver)];
    C --> D[Tabla Golden Analítica<br>(Gold - Delta Lake)];
    D --> E[Visualizaciones & Insights];
    subgraph Databricks
        B;
        C;
        D;
        E;
    end
    style A fill:#f9f9f9,stroke:#333,stroke-width:2px
    style D fill:#e6f7ff,stroke:#007bff,stroke-width:2px
```

## Estructura del Repositorio

El repositorio está organizado para promover la modularidad, la reproducibilidad y la claridad.

```
.
├── README.md                   # Este archivo. La documentación principal del proyecto.
├── LICENSE                     # Licencia MIT.
├── .gitignore                  # Archivos y directorios ignorados por Git.
├── notebooks/
│   └── databricks_movie_analysis.py # El notebook de Databricks exportado como script Python.
├── src/
│   └── futuro_etl_functions.py        # Funciones reutilizables para la lógica ETL.
├── docs/
│   └── futuro_architecture_diagram.png # Diagrama de la arquitectura del pipeline.
└── data/
    ├── raw/                    # Datos originales sin procesar.
    └── processed/              # Datos transformados y listos para análisis (almacenados en Delta Lake).
```

## Tecnologías Utilizadas

*   **Databricks:** Plataforma unificada para el análisis de datos y la ingeniería.
*   **Apache Spark:** Motor de procesamiento distribuido a gran escala.
*   **PySpark:** API de Python para Spark.
*   **Spark SQL:** Lenguaje para consultas y transformaciones declarativas.
*   **Delta Lake:** Capa de almacenamiento de código abierto que aporta transacciones ACID, Time Travel y optimizaciones a los data lakes.

## Cómo Reproducir este Proyecto

Sigue estos pasos para ejecutar el pipeline en tu propio entorno de Databricks.

1.  **Clonar el Repositorio:**
    ```bash
    git clone https://github.com/TU_USUARIO/databricks-movielens-analysis.git
    cd databricks-movielens-analysis
    ```

2.  **Configurar el Entorno en Databricks:**
    *   Crea una cuenta gratuita en [Databricks Community Edition](https://community.cloud.databricks.com/).
    *   Crea un nuevo cluster (ej. con runtime 13.3 LTS o superior).
    *   Crea un nuevo notebook y adjúntalo al cluster.

3.  **Preparar los Datos:**
    *   Descarga el [MovieLens Small Dataset](https://grouplens.org/datasets/movielens/latest/).
    *   Sube los archivos `movies.csv` y `ratings.csv` a un **Volume** en tu Databricks Workspace. Una ruta recomendada es: `/Volumes/workspace/movie/movielens/`.
    *   Ajusta la variable `base_path` en el notebook si usas una ruta diferente.

4.  **Ejecutar el Pipeline:**
    *   Copia el contenido de `notebooks/databricks_movie_analysis.py` y pégalo en las celdas de tu notebook de Databricks.
    *   Ejecuta las celdas en orden. El script guardará la tabla final en formato Delta en la ruta `/Volumes/workspace/movie/delta_tables/movielens_analyzed_final`.

## Análisis y Principales Insights

El proyecto concluye con un análisis exploratorio sobre la tabla final, revelando tendencias clave:

*   **Géneros más Populares:** Los géneros de **Drama** y **Comedia** dominan en número de valoraciones, indicando una fuerte preferencia de la audiencia.
*   **Calidad vs. Antigüedad:** No existe una correlación clara entre el año de una película y su rating promedio. Tanto los clásicos como los estrenos modernos pueden recibir altas calificaciones.

<img width="1117" height="359" alt="visualization1" src="https://github.com/user-attachments/assets/a672fcfc-8ad5-4280-b54a-f8774e4c9e4c" />
<img width="1117" height="359" alt="visualization2" src="https://github.com/user-attachments/assets/da28ea73-8725-4cca-9b0e-161cb0060bb5" />


## Buenas Prácticas y Habilidades Demostradas

Este proyecto no es solo un ETL; es una demostración de principios de ingeniería de datos moderna:

*   **Definición de Schemas:** Uso explícito de `StructType` para asegurar la calidad y el rendimiento en la lectura de datos.
*   **Modularidad:** Separación de la lógica de extracción, transformación y carga en pasos claros y documentados.
*   **Calidad de Datos:** Proceso activo de limpieza, incluyendo la eliminación de duplicados, el manejo de valores nulos, la validación de datos y manejo de casos específicos.
*   **Arquitectura Moderna:** Uso de **Unity Catalog Volumes** para un almacenamiento gestionado y seguro.
*   **Almacenamiento Optimizado:** Implementación de **Delta Lake** con particionamiento (`partitionBy`) para acelerar consultas y la funcionalidad de **Time Travel** para la recuperación de datos.

## Mejoras Futuras

*   **Ingeniería de Features:** Crear una tabla de características para alimentar un modelo de Machine Learning.
*   **Sistema de Recomendación:** Desarrollar un modelo de filtrado colaborativo para sugerir películas a los usuarios.
*   **Orquestación:** Automatizar el pipeline con herramientas como Airflow o Databricks Jobs para una ejecución programada.
*   **Dashboarding:** Conectar la tabla Delta a una herramienta como Power BI o Tableau para crear un dashboard interactivo.

## Licencia

Este proyecto está bajo la Licencia MIT. Consulta el archivo `LICENSE` para más detalles.
