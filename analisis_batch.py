"""
Tarea 3 - Big Data (UNAD)
Procesamiento en batch con Apache Spark (PySpark)
Tema: Hurto en Colombia (Policía Nacional - DIJIN, datos.gov.co)

Uso:
    1) Descargar los CSV de "Reporte Hurto por Modalidades" desde datos.gov.co
    2) Guardarlos en la carpeta  datos/
    3) Ejecutar:  python analisis_batch.py
"""

import glob
import os
import re
import unicodedata

import matplotlib
matplotlib.use("Agg")  # permite guardar gráficos sin pantalla
import matplotlib.pyplot as plt

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

RUTA_DATOS = "datos/*.csv"  # todos los CSV de esta carpeta
CARPETA_SALIDA = "resultados"
os.makedirs(CARPETA_SALIDA, exist_ok=True)


# ----------------------------------------------------------------------
# 0. Sesión de Spark
# ----------------------------------------------------------------------
spark = (
    SparkSession.builder
    .appName("Tarea3_Hurto_Colombia_Batch")
    .config("spark.sql.ansi.enabled", "false")  # fechas mal formadas -> null, no error
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")


# ----------------------------------------------------------------------
# 1. Carga de datos desde la fuente original (CSV de datos.gov.co)
#    Cada archivo se lee por separado porque el orden de las columnas
#    puede cambiar entre conjuntos de datos.
# ----------------------------------------------------------------------
def normalizar(nombre):
    """'FECHA HECHO' -> 'fecha_hecho' (sin tildes, espacios ni símbolos)."""
    nombre = unicodedata.normalize("NFKD", nombre).encode("ascii", "ignore").decode()
    return re.sub(r"[^a-z0-9]+", "_", nombre.lower()).strip("_")


def buscar(columnas, clave):
    """Devuelve la primera columna cuyo nombre contenga la palabra clave."""
    for c in columnas:
        if clave in c:
            return c
    return None


def cargar_archivo(ruta):
    d = (spark.read.option("header", True).option("encoding", "UTF-8").csv(ruta))
    d = d.toDF(*[normalizar(c) for c in d.columns])
    cols = d.columns
    c_fecha = buscar(cols, "fecha")
    c_depto = buscar(cols, "departamento")
    c_muni = buscar(cols, "municipio")
    c_arma = buscar(cols, "arma")
    c_cant = buscar(cols, "cantidad")
    c_tipo = buscar(cols, "tipo")
    print(f"{os.path.basename(ruta)} -> columnas:", cols)
    return d.select(
        F.col(c_fecha).cast("string").alias("fecha_txt"),
        F.upper(F.trim(F.col(c_depto).cast("string"))).alias("departamento"),
        F.upper(F.trim(F.col(c_muni).cast("string"))).alias("municipio"),
        (F.upper(F.trim(F.col(c_arma).cast("string"))) if c_arma
         else F.lit("SIN DATO")).alias("arma_medio"),
        (F.upper(F.trim(F.col(c_tipo).cast("string"))) if c_tipo
         else F.lit("HURTO")).alias("tipo_hurto"),
        (F.col(c_cant).cast("int") if c_cant else F.lit(1)).alias("cantidad"),
    )


archivos = sorted(glob.glob(RUTA_DATOS))
if not archivos:
    raise SystemExit("No se encontraron CSV en la carpeta 'datos/'.")

print("\n===== 1. CARGA =====")
df = None
for ruta in archivos:
    parcial = cargar_archivo(ruta)
    df = parcial if df is None else df.unionByName(parcial)
print("Registros cargados:", df.count())
df.printSchema()


# ----------------------------------------------------------------------
# 2. Limpieza y transformación
# ----------------------------------------------------------------------
# Se quita el prefijo "ARTÍCULO 239." y el sufijo "(CT)" de los tipos de hurto
df = df.withColumn("municipio", F.trim(F.regexp_replace("municipio", r"\(CT\)", "")))
df = df.withColumn(
    "tipo_hurto",
    F.trim(F.regexp_replace(F.regexp_replace("tipo_hurto", r"^ART.CULO\s*\d+\.?\s*", ""),
                            r"\(CT\)", "")),
)

# 2.3 Fecha: formato dd/MM/yyyy (ej. 05/03/2014 = 5 de marzo de 2014)
fecha = F.coalesce(
    F.to_date(F.substring("fecha_txt", 1, 10), "dd/MM/yyyy"),
    F.to_date(F.substring("fecha_txt", 1, 10), "yyyy-MM-dd"),
)
df = df.withColumn("fecha", fecha).drop("fecha_txt")

# 2.4 Limpieza: nulos y cantidades inválidas (no se eliminan duplicados: cada fila
#     puede ser un evento distinto con los mismos valores)
antes = df.count()
df = (
    df.dropna(subset=["fecha", "departamento", "municipio"])
      .fillna({"arma_medio": "SIN DATO", "cantidad": 1})
      .filter(F.col("cantidad") > 0)
)
despues = df.count()
print(f"\n===== 2. LIMPIEZA =====\nAntes: {antes} | Después: {despues} | "
      f"Eliminados: {antes - despues}")

# 2.5 Variables derivadas
df = (
    df.withColumn("anio", F.year("fecha"))
      .withColumn("mes", F.month("fecha"))
      .withColumn("dia_semana", F.dayofweek("fecha"))  # 1=domingo ... 7=sábado
)
df.cache()
df.show(5, truncate=False)


# ----------------------------------------------------------------------
# 3. Análisis exploratorio (EDA) con DataFrames
# ----------------------------------------------------------------------
print("\n===== 3. EDA =====")
print("Total de hurtos (suma de cantidad):",
      df.agg(F.sum("cantidad")).collect()[0][0])
df.select("cantidad").describe().show()

top_deptos = (df.groupBy("departamento").agg(F.sum("cantidad").alias("total"))
                .orderBy(F.desc("total")).limit(10))
top_munis = (df.groupBy("municipio").agg(F.sum("cantidad").alias("total"))
               .orderBy(F.desc("total")).limit(10))
por_anio = (df.groupBy("anio").agg(F.sum("cantidad").alias("total")).orderBy("anio"))
por_mes = (df.groupBy("mes").agg(F.sum("cantidad").alias("total")).orderBy("mes"))
por_dia = (df.groupBy("dia_semana").agg(F.sum("cantidad").alias("total"))
             .orderBy("dia_semana"))
por_tipo = (df.groupBy("tipo_hurto").agg(F.sum("cantidad").alias("total"))
              .orderBy(F.desc("total")).limit(10))
top_armas = (df.groupBy("arma_medio").agg(F.sum("cantidad").alias("total"))
               .orderBy(F.desc("total")).limit(10))

for titulo, tabla in [("Top 10 departamentos", top_deptos),
                      ("Top 10 municipios", top_munis),
                      ("Hurtos por año", por_anio),
                      ("Hurtos por mes", por_mes),
                      ("Hurtos por día de la semana", por_dia),
                      ("Top 10 tipos de hurto", por_tipo),
                      ("Top 10 armas/medios", top_armas)]:
    print(f"\n--- {titulo} ---")
    tabla.show(truncate=False)


# ----------------------------------------------------------------------
# 4. Ejemplo con RDDs (transformaciones y acciones)
# ----------------------------------------------------------------------
print("\n===== 4. EJEMPLO CON RDD =====")
rdd = df.select("departamento", "cantidad").rdd.map(lambda r: (r[0], r[1]))  # map
rdd_reducido = rdd.reduceByKey(lambda a, b: a + b)                            # reduceByKey
print("Departamentos distintos (count):", rdd_reducido.count())                # count
print("Top 5 (take):", rdd_reducido.sortBy(lambda x: -x[1]).take(5))           # take
print("Total general (reduce):", rdd.map(lambda x: x[1]).reduce(lambda a, b: a + b))  # reduce


# ----------------------------------------------------------------------
# 5. Visualización (se convierten solo los resultados agregados a pandas)
# ----------------------------------------------------------------------
print("\n===== 5. GRÁFICOS =====")
DIAS = {1: "Dom", 2: "Lun", 3: "Mar", 4: "Mié", 5: "Jue", 6: "Vie", 7: "Sáb"}


def barras(pdf, x, y, titulo, archivo, horizontal=False, color="#2B59C3"):
    plt.figure(figsize=(10, 6))
    if horizontal:
        plt.barh(pdf[x].astype(str)[::-1], pdf[y][::-1], color=color)
        plt.xlabel("Número de hurtos")
    else:
        plt.bar(pdf[x].astype(str), pdf[y], color=color)
        plt.ylabel("Número de hurtos")
    plt.title(titulo, fontsize=14, fontweight="bold")
    plt.tight_layout()
    ruta = os.path.join(CARPETA_SALIDA, archivo)
    plt.savefig(ruta, dpi=150)
    plt.close()
    print("Guardado:", ruta)


barras(top_deptos.toPandas(), "departamento", "total",
       "Top 10 departamentos con más hurtos", "01_top_departamentos.png",
       horizontal=True)
barras(top_munis.toPandas(), "municipio", "total",
       "Top 10 municipios con más hurtos", "02_top_municipios.png",
       horizontal=True, color="#14A38B")
barras(por_anio.toPandas(), "anio", "total",
       "Hurtos por año", "03_hurtos_por_anio.png")
barras(por_mes.toPandas(), "mes", "total",
       "Hurtos por mes", "04_hurtos_por_mes.png", color="#EA580C")
pdf_dia = por_dia.toPandas()
pdf_dia["dia"] = pdf_dia["dia_semana"].map(DIAS)
barras(pdf_dia, "dia", "total",
       "Hurtos por día de la semana", "05_hurtos_por_dia.png", color="#6D4AD8")
barras(por_tipo.toPandas(), "tipo_hurto", "total",
       "Top 10 tipos de hurto", "07_tipos_de_hurto.png", horizontal=True, color="#EA580C")
barras(top_armas.toPandas(), "arma_medio", "total",
       "Top 10 armas o medios utilizados", "06_armas_medios.png",
       horizontal=True, color="#64748B")


# ----------------------------------------------------------------------
# 6. Guardar datos limpios
# ----------------------------------------------------------------------
df.write.mode("overwrite").parquet(os.path.join(CARPETA_SALIDA, "hurtos_limpios_parquet"))
print("\nDatos limpios guardados en", os.path.join(CARPETA_SALIDA, "hurtos_limpios_parquet"))

spark.stop()
print("\nProceso batch finalizado.")
