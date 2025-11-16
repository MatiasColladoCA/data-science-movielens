#!/usr/bin/env python
# coding: utf-8

# # Vista rapida de datasets con Spark SQL

# In[ ]:


# El SparkSession ya existe en el notebook de Databricks por defecto
csv_files = [
    "genome-scores.csv",
    "genome-tags.csv",
    "links.csv",
    "movies.csv",
    "ratings.csv",
    "tags.csv"
]

base_path = "/Volumes/workspace/movie/movielens/"


# In[ ]:


# Crear vistas temporales para poder consultarlas con SQL
movies_df.createOrReplaceTempView("movies_view")
ratings_df.createOrReplaceTempView("ratings_view")

# --- Consultas Básicas con Spark SQL ---

# 1. Ver las primeras 10 películas
print("Primeras 10 películas:")
spark.sql("SELECT title, genres FROM movies_view LIMIT 10").show(truncate=False)

# 2. Encontrar todas las películas de 'Toy Story'
print("\nPelículas de 'Toy Story':")
spark.sql("SELECT * FROM movies_view WHERE title LIKE 'Toy Story%'").show(truncate=False)

# 3. Ver los ratings más altos (por encima de 4.5)
print("\nRatings más altos:")
spark.sql("SELECT userId, movieId, rating FROM ratings_view WHERE rating > 4.5 ORDER BY rating DESC").show(10)


# # Carga Optimizada y Definición de Schema

# In[ ]:


# --- Definición de Schemas ---
# Evita errores como por ejemplo suma de strings y agiliza la lectura
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType, TimestampType

# Schema para movies.csv
movies_schema = StructType([
    StructField("movieId", IntegerType(), True),
    StructField("title", StringType(), True),
    StructField("genres", StringType(), True)
])

# Schema para ratings.csv
ratings_schema = StructType([
    StructField("userId", IntegerType(), True),
    StructField("movieId", IntegerType(), True),
    StructField("rating", FloatType(), True),
    StructField("timestamp", IntegerType(), True) # Lo leeremos como int y luego transformaremos
])

# --- Carga de Datos con Schemas ---
base_path = "/Volumes/workspace/movie/movielens/"

# Cargar los datos principales usando los schemas definidos
movies_df = spark.read.format("csv").option("header", "true").schema(movies_schema).load(base_path + "movies.csv")
ratings_df = spark.read.format("csv").option("header", "true").schema(ratings_schema).load(base_path + "ratings.csv")

print("Datos de películas cargados con schema:")
movies_df.printSchema()
movies_df.show(5, truncate=False)

print("\nDatos de ratings cargados con schema:")
ratings_df.printSchema()
ratings_df.show(5, truncate=False)


# In[ ]:


display(movies_df.select("genres").distinct())


# # Transformación y Limpieza

# ## Eliminar duplicados

# In[ ]:


from pyspark.sql import DataFrame

def remove_duplicates(df: DataFrame, subset_cols: list, df_name: str) -> DataFrame:
    """
    Identifica y elimina filas duplicadas de un DataFrame basándose en un subconjunto de columnas. La función agrega una capa de auditación y reporte que dropDuplicates no logra.

    Args:
        df (DataFrame): El DataFrame de Spark a limpiar.
        subset_cols (list): Lista de nombres de columna para considerar la unicidad.
        df_name (str): Nombre descriptivo del DataFrame, usado para el logging.

    Returns:
        DataFrame: Un nuevo DataFrame con los duplicados eliminados.
    """
    initial_count = df.count()
    unique_count = df.select(subset_cols).distinct().count()
    
    duplicates_found = initial_count - unique_count
    
    print(f"--- Limpieza de Duplicados para: {df_name} ---")
    print(f"Filas iniciales: {initial_count}")
    print(f"Filas únicas basadas en {subset_cols}: {unique_count}")
    print(f"Duplicados a eliminar: {duplicates_found}\n")
    
    if duplicates_found > 0:
        df_cleaned = df.dropDuplicates(subset_cols)
        print(f"Duplicados eliminados. Nuevo total de filas: {df_cleaned.count()}\n")
        return df_cleaned
    else:
        print("No se encontraron duplicados. El DataFrame no fue modificado.\n")
        return df


# In[ ]:


# --- Limpieza de Duplicados ---
# Eliminamos posibles películas duplicadas antes de cualquier transformación.
movies_df_clean = remove_duplicates(df=movies_df, subset_cols=['movieId'], df_name='Movies DataFrame Original')

# # Eliminamos posibles películas duplicadas antes de cualquier transformación.
# movies_df_clean = remove_duplicates(df=movies_df, subset_cols=['movieId'], df_name='Movies DataFrame Original')


# ## Separación de año y generos

# In[ ]:


from pyspark.sql.functions import col, split, explode, regexp_extract, from_unixtime, to_timestamp, lit, when

# --- Transformación del DataFrame de Películas (Versión Profundamente Limpia) ---
# 1. Extraer el año de forma segura (usando try_cast).
# 2. Separar los géneros.
# 3. Convertir el marcador "(no genres listed)" a NULL para consistencia.

movies_transformed_df = movies_df_clean.withColumn(
    "year",
    regexp_extract(col("title"), r"\((\d{4})\)", 1).try_cast("int")
).withColumn(
    "genre",
    explode(split(col("genres"), "\\|"))
).withColumn(
    # Reemplazamos el texto de marcador por un verdadero NULL
    "genre",
    when(col("genre") == "(no genres listed)", lit(None)).otherwise(col("genre"))
).select(
    "movieId",
    "title",
    "year",
    "genre"
)

print("Películas transformadas (géneros explotados y valores nulos estandarizados):")
movies_transformed_df.show(10, truncate=False)


# --- Transformación del DataFrame de Ratings (Versión Limpia) ---
# 1. Convertir el timestamp de Unix Epoch a un formato de fecha legible.
# 2. Convertir ratings inválidos (nulos o 0.0) a NULL.

ratings_transformed_df = ratings_df.withColumn(
    "rating_timestamp",
    to_timestamp(from_unixtime(col("timestamp")))
).withColumn(
    # Si el rating es nulo o 0.0, se convierte a NULL. De lo contrario, se mantiene el valor.
    "rating",
    when((col("rating").isNull()) | (col("rating") <= 0.0), lit(None)).otherwise(col("rating"))
).select(
    "userId",
    "movieId",
    "rating",
    "rating_timestamp"
)

print("\nRatings transformados y limpios:")
ratings_transformed_df.show(10, truncate=False)


# ## Buscar valores únicos para entender y limpiar mejor las columnas

# In[ ]:


from pyspark.sql import DataFrame

def show_unique_values(df: DataFrame, df_name: str, limit: int = None, ignore_cols: list = []):
    """
    Muestra los valores únicos de cada columna de un DataFrame para su validación.
    
    Args:
        df (DataFrame): El DataFrame de Spark a analizar.
        df_name (str): El nombre del DataFrame para imprimir en el encabezado.
        limit (int, optional): Número máximo de valores únicos a mostrar por columna.
                               Si es None, muestra todos. Por defecto es None.
        ignore_cols (list): Lista de nombres de columna que se omitirán en el análisis.
    """
    print(f"--- Validación de Valores Únicos para: {df_name} ---\n")
    
    for column_name in df.columns:
        if column_name in ignore_cols:
            continue
        print(f"Columna: '{column_name}'")
        
        if limit is not None:
            # Muestra solo el número limitado de valores
            df.select(column_name).distinct().show(limit, truncate=False)
        else:
            # Muestra TODOS los valores únicos
            # Advertencia: Esto puede ser lento o consumir mucha memoria para columnas con muchos valores únicos.
            unique_values = [row[column_name] for row in df.select(column_name).distinct().collect()]
            for value in unique_values:
                print(value)
                
        print("-" * 40) # Separador para mayor claridad


# In[ ]:


# Mostrar valores únicos para el DataFrame de películas
ignore = ["movieId", "title"]
show_unique_values(movies_transformed_df, "Movies Transformado", ignore_cols=ignore)


# In[ ]:


# Mostrar valores únicos para el DataFrame de ratings
ignore = ["rating_timestamp", "movieId", "userId"]
show_unique_values(ratings_transformed_df, "Ratings Transformado", ignore_cols=ignore)


# In[ ]:


from pyspark.sql.functions import col
# --- Paso de Limpieza Avanzada: Eliminar Títulos Colados en Géneros ---

# 1. Primero, inspeccionemos qué filas coinciden con nuestro patrón sospechoso.
#    Usamos .rlike() para aplicar la expresión regular.
#    La regex busca cualquier string que contenga "(4 dígitos)" y luego una comilla doble.
print("🔍 Filas identificadas con un posible título en la columna 'genre':")
suspicious_rows_df = movies_transformed_df.filter(col("genre").rlike('.*\(\d{4}\).*"'))
suspicious_rows_df.show(truncate=False)

# 2. Ahora, eliminamos estas filas del DataFrame.
#    El símbolo '~' es el operador "NOT", por lo que filtramos para mantener todo lo que NO coincide con el patrón.
movies_cleaned_df = movies_transformed_df.filter(~col("genre").rlike('.*\(\d{4}\).*"'))

print(f"\n✅ Se han eliminado {suspicious_rows_df.count()} filas incorrectas.")


# ## Buscar valores nulos y borrarlos

# In[ ]:


from pyspark.sql.functions import count, when, col

total_count_movies = movies_transformed_df.count()

# Calcular y mostrar la proporción de nulos para cada columna de forma unificada
movies_transformed_df.select(
    [(count(when(col(c).isNull(), c)) / total_count_movies).alias(c) for c in movies_transformed_df.columns]
).show()

total_count_ratings = ratings_transformed_df.count()

# Calcular y mostrar la proporción de nulos para cada columna
ratings_transformed_df.select(
    [(count(when(col(c).isNull(), c)) / total_count_movies).alias(c) for c in ratings_transformed_df.columns]
).show()


# In[ ]:


from pyspark.sql import DataFrame

def drop_nulls_and_report(df: DataFrame, df_name: str) -> DataFrame:
    """
    Elimina todas las filas que contienen al menos un valor nulo y reporta
    la cantidad de filas eliminadas.

    Args:
        df (DataFrame): El DataFrame de Spark a limpiar.
        df_name (str): Nombre descriptivo del DataFrame para el logging.

    Returns:
        DataFrame: Un nuevo DataFrame sin filas nulas.
    """
    initial_count = df.count()
    
    print(f"--- Eliminación de Nulos para: {df_name} ---")
    print(f"Filas iniciales: {initial_count}")
    
    # na.drop() elimina cualquier fila que contenga un valor nulo en CUALQUIER columna
    df_cleaned = df.na.drop()
    
    final_count = df_cleaned.count()
    rows_dropped = initial_count - final_count
    
    print(f"Filas eliminadas con nulos: {rows_dropped}")
    print(f"Filas finales: {final_count}\n")
    
    return df_cleaned


# In[ ]:


# Limpiar el DataFrame de películas
movies_clean_df = drop_nulls_and_report(movies_transformed_df, "Movies Transformado")

# Limpiar el DataFrame de ratings
ratings_clean_df = drop_nulls_and_report(ratings_transformed_df, "Ratings Transformado")


# ## Agregación y Joins simples

# In[ ]:


# --- Agregaciones y Joins Simples ---

# Spark SQL: Contar el número total de ratings
total_ratings_sql = spark.sql("SELECT COUNT(*) as total_ratings FROM ratings_view").collect()[0][0]
print(f"Total de ratings en la tabla (SQL): {total_ratings_sql}")

# DataFrame API: Hacer lo mismo
total_ratings_df = ratings_df.count()
print(f"Total de ratings en la tabla (DataFrame API): {total_ratings_df}")

# Spark SQL: Encontrar las 5 películas con más ratings
print("\nTop 5 películas con más ratings (SQL):")
spark.sql("""
    SELECT
        movieId,
        COUNT(*) as num_ratings
    FROM ratings_view
    GROUP BY movieId
    ORDER BY num_ratings DESC
    LIMIT 5
""").show()

# Join simple con Spark SQL: Unir una película con sus ratings
print("\nJoin simple: Ver los ratings para la película 'Jumanji':")
spark.sql("""
    SELECT
        m.title,
        r.userId,
        r.rating
    FROM movies_view m
    JOIN ratings_view r ON m.movieId = r.movieId
    WHERE m.title = 'Jumanji (1995)'
""").show()


# # Guardar en formato Delta Lake

# ## Unión de Datos

# In[ ]:


# --- Paso 1: Unir los dos DataFrames en una tabla "Golden" ---
# Usamos un 'inner join' para quedarnos solo con los ratings que tienen una película asociada.

# Es una buena práctica renombrar las columnas antes del join para evitar ambigüedades,
# aunque en este caso 'movieId' es la única en común y es la clave del join.
# El join se realizará automáticamente en esta columna.

golden_df = ratings_transformed_df.join(
    movies_transformed_df,
    on="movieId",
    how="inner"
)

print("Vista previa de la tabla 'Golden' unida y desnormalizada:")
golden_df.printSchema()
golden_df.show(5, truncate=False)


# ## Guardar versión final

# In[ ]:


# --- Paso 2: Guardar la Tabla Golden en formato Delta Lake (Corregido) ---

delta_path = "/Volumes/workspace/movie/movielens/delta_tables/movielens_analyzed_final"
# Particionamos por 'year' porque es una columna de baja cardinalidad
# y muy común para filtrar en análisis de series temporales.
(golden_df.write
 .format("delta")
 .mode("overwrite")
 .partitionBy("year")
 .option("overwriteSchema", "true")
 .save(delta_path))

print(f"✅ Tabla 'Golden' guardada exitosamente en Delta Lake en: {delta_path}")

# --- Validación Final ---
print("\n🔍 Validación: Leyendo la tabla final desde Delta Lake:")
final_df = spark.read.format("delta").load(delta_path)
final_df.show(5)
final_df.printSchema()


# # Visualización

# ## Géneros mas populares

# In[ ]:


from pyspark.sql.functions import count, avg, desc

# Calcular el número de ratings por género
genre_popularity_df = golden_df.groupBy("genre").agg(count("rating").alias("num_ratings")).orderBy(desc("num_ratings"))

print("Popularidad de los géneros por número de ratings:")
genre_popularity_df.show()

# --- Visualización ---
display(genre_popularity_df)


# In[ ]:


# Calcular el rating promedio por año
avg_rating_by_year_df = golden_df.filter(col("year").isNotNull()).groupBy("year").agg(avg("rating").alias("avg_rating")).orderBy("year")

print("Rating promedio de las películas por año de lanzamiento:")
avg_rating_by_year_df.show(20)

# --- Visualización ---
display(avg_rating_by_year_df)


# # Concusiones
# 
# A partir del análisis del dataset de MovieLens, se pueden extraer las siguientes conclusiones simples:
# 
# 1.  **Dominancia de Géneros:** Los géneros de **Drama** y **Comedia** son los que concentran la mayor cantidad de ratings, lo que sugiere una fuerte preferencia del público por este tipo de contenido.
# 
# 2.  **Calidad vs. Antigüedad:** El análisis del rating promedio por año muestra que entre las decadas del 20 y el 80 las películas tuvieron un rating algo sostenido a diferencia de epocas anteriores y posteriores.
