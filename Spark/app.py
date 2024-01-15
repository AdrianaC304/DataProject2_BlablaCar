
#Copitar app al contenedor de docker
#docker cp -L app.py docker-spark-master-1:/opt/bitnami/spark/app.py


#Ejecutar app dentro contenedor de docker(nodo Spark)
#docker exec docker-spark-master-1 spark-submit --master spark://172.20.0.2:7077 app.py


from pyspark.sql import SparkSession
from pyspark.sql.functions import rand

# Create a SparkSession
spark = SparkSession.builder \
    .appName("My App") \
    .getOrCreate()

# Crear un DataFrame con 100 filas y una columna llamada 'random_data'
df = spark.range(1, 101).select(rand().alias('random_data'))

# Mostrar el DataFrame
df.show()

# Stop the SparkSession
spark.stop()
