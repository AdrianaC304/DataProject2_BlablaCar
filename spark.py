#pip install pyspark


####################################################################
####################  Leer un topic desde Kafka  ###################
####################################################################

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType

# Crea una sesión de Spark
spark = SparkSession.builder.appName("LecturaDesdeTopic").getOrCreate()

# Define el esquema de los datos que esperas leer desde el topic
schema = StructType([
    StructField("index", IntegerType()),
    StructField("latitud", DoubleType()),
    StructField("longitud", DoubleType())
])

# Lee datos desde el topic "rutas" usando Spark
df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "rutas") \
    .load()

# Decodifica los datos en formato JSON utilizando el esquema definido
df = df.selectExpr("CAST(value AS STRING)").select(from_json("value", schema).alias("datos")).select("datos.*")

# Puedes realizar operaciones adicionales con los datos según tus necesidades

# Muestra los resultados en la consola (puedes cambiar esto según tus necesidades)
query = df.writeStream.outputMode("append").format("console").start()

# Espera hasta que la consulta termine
query.awaitTermination()
