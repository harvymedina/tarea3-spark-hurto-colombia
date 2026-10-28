# Análisis batch del hurto en Colombia con Apache Spark

Trabajo de la Tarea 3 del curso **Big Data** (UNAD, código 202016911).
Autor: **Harvy Duvan Pacheco Medina**

## Problema

El hurto afecta de forma distinta a cada región y época del año. Este proyecto busca responder, con datos abiertos de la Policía Nacional:

- ¿En qué departamentos y municipios se concentran más los hurtos de automotores, motocicletas, comercio y residencias?
- ¿Cómo han cambiado los casos con los años y en qué meses y días de la semana ocurren más?
- ¿Qué armas o medios se usan con mayor frecuencia?

## Conjunto de datos

Fuente: *Reporte Hurto por Modalidades – Policía Nacional* (DIJIN), publicado en [datos.gov.co](https://www.datos.gov.co). Se usan dos conjuntos, que juntos superan 1,3 millones de registros:

| Archivo | Contenido | Enlace |
|---|---|---|
| `hurto_vehiculos.csv` | Hurto de automotores y motocicletas | https://www.datos.gov.co/d/9vha-vh9n |
| `hurto_comercio_residencias.csv` | Hurto a entidades comerciales y residencias | https://www.datos.gov.co/d/6sqw-8cg5 |

Columnas principales: departamento, municipio, código DANE, armas o medios, fecha del hecho, género, grupo etario, tipo de hurto y cantidad.

## Tecnologías

- Python 3.12
- Apache Spark (PySpark)
- Matplotlib y pandas (solo para graficar resultados ya agregados)

## Qué hace `analisis_batch.py`

1. **Carga** cada CSV por separado y los une por nombre de columna.
2. **Limpia y transforma**: nombres de columnas sin tildes ni espacios, textos en mayúsculas, se quitan el prefijo "ARTÍCULO 239." y el sufijo "(CT)", las fechas se convierten a tipo fecha (formato dd/MM/yyyy), se eliminan registros sin fecha, departamento o municipio, y se crean las variables año, mes y día de la semana.
3. **Análisis exploratorio (EDA)** con DataFrames: totales, estadísticas de la cantidad y conteos por departamento, municipio, año, mes, día de la semana, tipo de hurto y arma o medio.
4. **Ejemplo con RDDs**: `map`, `reduceByKey`, `count`, `take` y `reduce` sobre departamento y cantidad.
5. **Visualización**: genera 7 gráficos PNG en la carpeta `resultados/`.
6. **Guarda** los datos limpios en formato Parquet.

## Cómo ejecutarlo

Requisitos: Java 17 y Python 3.12 (en Mac: `brew install openjdk@17 python@3.12`).

```bash
# 1. Crear y activar el entorno virtual
python3.12 -m venv venv
source venv/bin/activate

# 2. Instalar dependencias
pip install pyspark matplotlib pandas

# 3. Descargar los datos
mkdir -p datos
curl -L -o datos/hurto_vehiculos.csv "https://www.datos.gov.co/api/views/9vha-vh9n/rows.csv?accessType=DOWNLOAD"
curl -L -o datos/hurto_comercio_residencias.csv "https://www.datos.gov.co/api/views/6sqw-8cg5/rows.csv?accessType=DOWNLOAD"

# 4. Ejecutar
python analisis_batch.py
```

Los gráficos quedan en `resultados/`.

## Estructura

```
.
├── analisis_batch.py
├── README.md
├── datos/          (no se sube a Git: archivos pesados)
└── resultados/     (gráficos y datos limpios)
```

## Resultados

Gráficos generados en `resultados/`:

1. `01_top_departamentos.png`: departamentos con más hurtos
2. `02_top_municipios.png`: municipios con más hurtos
3. `03_hurtos_por_anio.png`: evolución por año
4. `04_hurtos_por_mes.png`: distribución por mes
5. `05_hurtos_por_dia.png`: distribución por día de la semana
6. `06_armas_medios.png`: armas o medios más usados
7. `07_tipos_de_hurto.png`: tipos de hurto más frecuentes

## Referencias

- Policía Nacional de Colombia – DIJIN. (s. f.). *Reporte Hurto por Modalidades Policía Nacional* [Conjunto de datos]. Datos Abiertos Colombia. https://www.datos.gov.co/d/d4fr-sbn2
- The Apache Software Foundation. (s. f.). *PySpark documentation*. https://spark.apache.org/docs/latest/api/python/
