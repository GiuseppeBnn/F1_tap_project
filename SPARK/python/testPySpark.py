from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType
from pyspark.sql import SparkSession
from pyspark.ml.regression import LinearRegression
from pyspark.ml.feature import VectorAssembler
#from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml import Pipeline
from pyspark.sql.functions import col, expr, split, concat, lit

# Definisci lo schema dei dati di input
laptime_schema = StructType([
    StructField("PilotNumber", IntegerType(), nullable=False),
    StructField("LastLapTime", StringType(), nullable=True),
    StructField("Lap", IntegerType(), nullable=True)
])
lapTimeTotal_df = None
LastLapTime_df = None


def linearRegression(pilotNumber):
    global lapTimeTotal_df
    df = lapTimeTotal_df.where("PilotNumber = " + pilotNumber).selectExpr("Lap as Lap", "LastLapTime as LapTime")
    print("check1")
    df = df.withColumn("Seconds", (split(col("LapTime"), ":").getItem(0) * 60 + split(col("LapTime"), ":").getItem(1)))
    df = df.withColumn("Seconds", df["Seconds"].cast(FloatType()))
    df.show()
    #print("check2")
    #vectorAssembler = VectorAssembler(inputCols=["Lap"], outputCol="features")
    #lr = LinearRegression(featuresCol="features", regParam=0.01,labelCol=)
    #pipeline = Pipeline(stages=[vectorAssembler, lr])
    #print("check3")
    #(trainingData, testData) = df.randomSplit([0.8, 0.2], seed=42)
    #print("trainingData ",trainingData)
    #model = pipeline.fit(trainingData)

    #predictions = model.transform(testData)
    #predictions.select("Lap", "LapTime", "predictedLapTime").show()

 





def updateLapTimeTotal(df : DataFrame, epoch_id):
  
    global lapTimeTotal_df
    global LastLapTime_df
    if df.count() != 0:
        lapTimeTotal_df = lapTimeTotal_df.union(df)
        lapTimeTotal_df.show()
        limited_df = lapTimeTotal_df.limit(30)
        LastLapTime_df2 = limited_df.groupBy("PilotNumber").agg(max("Lap").alias("Lap"))
        LastLapTime_df2 = LastLapTime_df2.join(limited_df, ["PilotNumber", "Lap"], "inner")
        print("LastLapTime_df2")
        LastLapTime_df2.show()
    #lapTimeTotal_df.groupBy("PilotNumber").agg(min("LastLapTime").alias("BestLapTime")).show()
    #lapTimeTotal_df.groupBy("PilotNumber").agg(avg("LastLapTime").alias("AvgLapTime")).show()
        for row in df.collect():
            linearRegression(str(row.PilotNumber))
    
    

def main():
    spark = SparkSession.builder \
        .appName("SparkF1") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    
    global lapTimeTotal_df
    
    lapTimeTotal_df = spark.createDataFrame(spark.sparkContext.emptyRDD(), laptime_schema)
    global LastLapTime_df  
    LastLapTime_df = spark.createDataFrame(spark.sparkContext.emptyRDD(), laptime_schema)

    #df = spark \
        #.readStream \
        #.format("kafka") \
        #.option("kafka.bootstrap.servers", "192.168.1.122:9093") \
        #.option("subscribe", "LiveTimingData") \
        #.load()


    df = (spark.readStream
                    .format("kafka")
                    .option("kafka.bootstrap.servers", "pkc-4nmjv.francecentral.azure.confluent.cloud:9092")
                    .option("kafka.ssl.endpoint.identification.algorithm", "https")
                    .option("kafka.sasl.mechanism", "PLAIN")
                    .option("kafka.security.protocol", "SASL_SSL")
                    .option("kafka.sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username='{}' password='{}';".format("OZK7A2B5EBU2OMWI", "ODBBNwyLTXqOxfE77h+FNLLFa7KB/LakW7HuivBZoFP1fkXevp4tTvgqIuhxFLpr"))
                    .option("subscribe", "LiveTimingData")
                    .option("kafka.client.id", "client-1")
                    .option("spark.streaming.kafka.maxRatePerPartition", "5")
                    .option("startingOffsets", "earliest")
                    .option("kafka.session.timeout.ms", "10000")
                    .load() )    
  

    df2 = df.select(col("value").cast("string").alias("json"))

    laptime_df = df2.select(
        get_json_object("json", "$.PilotNumber").cast(IntegerType()).alias("PilotNumber"),
        get_json_object("json", "$.LastLapTime.Value").alias("LastLapTime"),
        get_json_object("json", "$.NumberOfLaps").cast(IntegerType()).alias("Lap")
    ).where("LastLapTime is not null")
    
    laptime_query = laptime_df.writeStream\
        .outputMode("append")\
        .foreachBatch(updateLapTimeTotal)\
        .start()

    laptime_query.awaitTermination()

    #query = df.writeStream.outputMode("append").format("console").start().awaitTermination()

if __name__ == "__main__":
    main()
