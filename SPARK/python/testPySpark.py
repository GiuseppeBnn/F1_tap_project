from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType
from pyspark.sql import SparkSession
from pyspark.ml.regression import LinearRegression
from pyspark.ml.feature import VectorAssembler
#from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml import Pipeline
from pyspark.sql.functions import col,split, concat, lit
import elasticsearch

# Definisci lo schema dei dati di input
laptime_schema = StructType([
    StructField("PilotNumber", IntegerType(), nullable=False),
    StructField("LastLapTime", StringType(), nullable=True),
    StructField("Lap", IntegerType(), nullable=True)
])
prevision_schema = StructType([
    StructField("PilotNumber", IntegerType(), nullable=False),
    StructField("Lap", IntegerType(), nullable=True),
    StructField("Seconds", FloatType(), nullable=True)
])
lapTimeTotal_df = None
LastLapTime_df = None
es=elasticsearch.Elasticsearch(hosts=["http://localhost:9200"])

def sendToES(predictions:DataFrame):
    pass
    global es
    prediction_json=predictions.toJSON().collect()
    for prediction in prediction_json:
        #es.index(index="predictions",body=prediction)
        pass
    


def linearRegression(pilotNumber):
    global lapTimeTotal_df
    df = lapTimeTotal_df.where("PilotNumber = " + pilotNumber).selectExpr("PilotNumber as PilotNumber","Lap as Lap", "LastLapTime as LapTime")
    print("Dataframe del pilota " + pilotNumber)
    df = df.withColumn("Seconds", (split(col("LapTime"), ":").getItem(0) * 60 + split(col("LapTime"), ":").getItem(1)))
    df = df.withColumn("Seconds", df["Seconds"].cast(FloatType()))
    df=df.select("Lap","Seconds")
    vectorAssembler = VectorAssembler(inputCols=["Lap"], outputCol="features")
    lr = LinearRegression(featuresCol="features", regParam=0.01,labelCol="Seconds")
    pipeline = Pipeline(stages=[vectorAssembler, lr])
    spark20=SparkSession.builder.appName("SparkF1").getOrCreate()
    NextLap=df.agg(max("Lap").alias("Lap")).collect()
    if(NextLap[0]["Lap"] is None):
        NextLap=1
    else:
        NextLap=NextLap[0]["Lap"]+1    
    NextLap_df = spark20.createDataFrame([(pilotNumber,NextLap, 0)],["PilotNumber","Lap","Seconds"])
    
    model = pipeline.fit(df)

    predictions = model.transform(NextLap_df)
   
    predictions = predictions.withColumn("prediction", concat( lit(floor(col("prediction")/60)), lit(":"), format_number((col("prediction")%60), 3)))
    predictions = predictions.withColumn("prediction", predictions["prediction"].cast(StringType()))
    predictions= predictions.selectExpr("PilotNumber as PilotNumber","Lap as NextLap","prediction as NextLapTimePrediction")
    predictions.show()

 





def updateLapTimeTotal(df : DataFrame, epoch_id):
  
    global lapTimeTotal_df
    global LastLapTime_df
    if df.count() != 0:
        lapTimeTotal_df = lapTimeTotal_df.union(df)
        #lapTimeTotal_df.show()
        limited_df = lapTimeTotal_df.limit(30)
        LastLapTime_df2 = limited_df.groupBy("PilotNumber").agg(max("Lap").alias("Lap"))
        LastLapTime_df2 = LastLapTime_df2.join(limited_df, ["PilotNumber", "Lap"], "inner")
        #print("Latest Lap Time per Pilot")
        #LastLapTime_df2.show()

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

    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "broker:29092") \
        .option("subscribe", "LiveTimingData") \
        .load()


    #df = (spark.readStream
    #                .format("kafka")
    #                .option("kafka.bootstrap.servers", "pkc-4nmjv.francecentral.azure.confluent.cloud:9092")
    #                .option("kafka.ssl.endpoint.identification.algorithm", "https")
    #                .option("kafka.sasl.mechanism", "PLAIN")
    #                .option("kafka.security.protocol", "SASL_SSL")
    #                .option("kafka.sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username='{}' password='{}';".format("OZK7A2B5EBU2OMWI", "ODBBNwyLTXqOxfE77h+FNLLFa7KB/LakW7HuivBZoFP1fkXevp4tTvgqIuhxFLpr"))
    #                .option("subscribe", "LiveTimingData")
    #                .option("group.id", "group-1")
    #                .option("spark.streaming.kafka.maxRatePerPartition", "5")
    #                .option("startingOffsets", "latest")
    #                .option("kafka.session.timeout.ms", "10000")
    #                .load() )    
  

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
