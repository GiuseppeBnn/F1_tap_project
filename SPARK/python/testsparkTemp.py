import pyspark.sql.functions as funcs
from pyspark import SparkFiles
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

def main():
    # Crea una sessione Spark
    spark = SparkSession.builder \
        .appName("Leggere dati JSON da Kafka") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    schema = """
    {
        "type": "struct",
        "fields": [
            {"name": "DriverNumber", "type": "integer"},
            {"name": "Sectors", "type": "string"},
            {"name": "GapToLeader", "type": "string"},
            {"name": "IntervalToPositionAhead", "type": "string"},
            {"name": "LastLapTime", "type": "string"},
            {"name": "NumberOfLaps", "type": "integer"},
            {"name": "Speeds", "type": "string"},
            {"name": "BestLapTime", "type": "string"}
        ]
    }
    """

    # Leggi i dati JSON da Kafka utilizzando Structured Streaming
    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "192.168.1.122:9093") \
        .option("subscribe", "LiveTimingData") \
        .load()

    # Decodifica i dati JSON e estrai i campi specifici
    df = df.selectExpr("CAST(value AS STRING) as json")

    # Definisci uno schema per il DataFrame
    schema_df = spark.read.schema(schema).option("multiLine", "true").json(spark.sparkContext.emptyRDD())

    # Applica lo schema al DataFrame con i dati JSON
    df = df.select(from_json(df.json, schema_df.schema).alias("data")).select("data.*")

    # Visualizza il DataFrame risultante
    query = df.writeStream \
        .outputMode("append") \
        .format("console") \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    main()
