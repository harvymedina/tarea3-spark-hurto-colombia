"""Spark Structured Streaming + Kafka: consume el topic 'hurtos',
cuenta eventos por departamento y tipo de hurto y actualiza un gráfico."""
import os
import tempfile

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

TOPIC = "hurtos"
SERVIDOR = "localhost:9092"
SALIDA = "resultados"
os.makedirs(SALIDA, exist_ok=True)

# El conector de Kafka debe coincidir con la versión de Spark instalada
paquete = f"org.apache.spark:spark-sql-kafka-0-10_2.13:{pyspark.__version__}"

spark = (
    SparkSession.builder
    .appName("Tarea3_Streaming_Hurtos")
    .config("spark.jars.packages", paquete)
    .config("spark.sql.shuffle.partitions", "4")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")

esquema = StructType([
    StructField("fecha_hora", StringType()),
    StructField("departamento", StringType()),
    StructField("municipio", StringType()),
    StructField("tipo_hurto", StringType()),
    StructField("arma_medio", StringType()),
    StructField("cantidad", IntegerType()),
])

# 1. Lectura del topic de Kafka (cada mensaje llega como bytes)
crudo = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", SERVIDOR)
    .option("subscribe", TOPIC)
    .option("startingOffsets", "latest")
    .load()
)

# 2. Se convierte el JSON a columnas
eventos = (
    crudo.select(F.from_json(F.col("value").cast("string"), esquema).alias("e"))
         .select("e.*")
)

# 3. Conteo acumulado por departamento y tipo de hurto
conteo = eventos.groupBy("departamento", "tipo_hurto").agg(F.sum("cantidad").alias("total"))


def procesar_lote(lote, id_lote):
    """Se ejecuta en cada micro-lote: imprime tablas y actualiza el gráfico."""
    pdf = lote.toPandas()
    if pdf.empty:
        return
    por_depto = pdf.groupby("departamento")["total"].sum().sort_values()
    por_tipo = pdf.groupby("tipo_hurto")["total"].sum().sort_values()
    print(f"\n===== Micro-lote {id_lote} | eventos acumulados: {int(pdf['total'].sum())} =====")
    print(por_depto.sort_values(ascending=False).to_string())
    print(por_tipo.sort_values(ascending=False).to_string())

    fig, ejes = plt.subplots(1, 2, figsize=(14, 6))
    ejes[0].barh(por_depto.index, por_depto.values, color="#2B59C3")
    ejes[0].set_title("Hurtos por departamento (tiempo real)")
    ejes[1].barh(por_tipo.index, por_tipo.values, color="#EA580C")
    ejes[1].set_title("Hurtos por tipo (tiempo real)")
    fig.suptitle(f"Spark Streaming + Kafka | eventos acumulados: {int(pdf['total'].sum())}",
                 fontweight="bold")
    fig.tight_layout()
    fig.savefig(os.path.join(SALIDA, "streaming_tiempo_real.png"), dpi=120)
    plt.close(fig)


consulta = (
    conteo.writeStream
    .outputMode("complete")
    .foreachBatch(procesar_lote)
    .trigger(processingTime="5 seconds")
    .option("checkpointLocation", tempfile.mkdtemp(prefix="chk_hurtos_"))
    .start()
)
print("Streaming iniciado. Esperando eventos del topic 'hurtos'... (Ctrl + C para detener)")
consulta.awaitTermination()
