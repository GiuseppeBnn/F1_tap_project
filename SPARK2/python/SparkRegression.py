from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType, TimestampType
from pyspark.sql import SparkSession
from pyspark.ml.regression import LinearRegression
from pyspark.ml.feature import VectorAssembler
# from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml import Pipeline
import requests as req
import json
import elasticsearch

#es = elasticsearch.Elasticsearch(hosts=["http://elasticsearch:9200"])
pilotDataframes = {}
pipeline=None

laptime_schema = StructType([
    StructField("PilotNumber", IntegerType(), True),
    StructField("Lap", IntegerType(), True),
    StructField("LastLapTime", StringType(), True),
    StructField("timestamp", TimestampType(), True)
])



def linearRegression(pilotNumber):
    global pilotDataframes
    global pipeline
    
    df=pilotDataframes[pilotNumber].orderBy("Lap", ascending=False).limit(5)
    if df.count() > 0:
        print("Dataframe del pilota " + str(pilotNumber))
        df = df.withColumn("Seconds", (split(col("LastLapTime"), ":").getItem(
        0) * 60 + split(col("LastLapTime"), ":").getItem(1)))
        df = df.withColumn("Seconds", df["Seconds"].cast(FloatType()))
        df.show()
        model = pipeline.fit(df)
        print("Modello del pilota " + str(pilotNumber) + " creato")
        spark_session = SparkSession.builder.appName("SparkF1").getOrCreate()
#
        NextLap = df.limit(1).collect()[0]["Lap"]+1
#
        ##NextLap = df.agg(max("Lap").alias("Lap")).collect()
        ##if (NextLap[0]["Lap"] is None):
        ##    NextLap = 1
        ##else:
        ##    NextLap = NextLap[0]["Lap"]+1
        NextLap_df = spark_session.createDataFrame([(pilotNumber, NextLap, 0,0)], [
                                        "PilotNumber", "Lap", "Seconds", "timestamp"])
        predictions = model.transform(NextLap_df)
        predictions.show()
        #predictions = predictions.selectExpr(
        #"PilotNumber as Pilot", "Lap as NextLap", "prediction")
        #predictions = predictions.withColumn(
        #"prediction", predictions["prediction"].cast(FloatType()))
        #predictions = predictions.withColumn(
        #"Pilot", predictions["Pilot"].cast(IntegerType()))
        #predictions = predictions.withColumn("@timestamp", current_timestamp())
        #predictions.show()
        ##sendToES(predictions, 1)
        

def makePilotList(pilotsRaw):

    pilots = []
    for i in range(20):
        if pilotsRaw[i]["permanentNumber"] == "33":
            pilots.append(1)
        else:
            pilots.append(int(pilotsRaw[i]["permanentNumber"]))
    return pilots


def getPilotsData():
    dbUrl = "http://ergast.com/api/f1/2023/drivers.json"
    PilotData = req.get(dbUrl)
    PilotJson = (json.loads(PilotData.text))[
        "MRData"]["DriverTable"]["Drivers"]
    pilotList = makePilotList(PilotJson)

    return pilotList


def preparePilotsDataframes():
    pilotList = getPilotsData()
    global pilotDataframes
    session=SparkSession.builder.appName("SparkF1").getOrCreate()
    for i in range(20):
        pilotDataframes[pilotList[i]] = session.createDataFrame(
            session.sparkContext.emptyRDD(), laptime_schema)
        


def sendToES(data: DataFrame, choose: int):
    global es
    if (choose == 1):
        data_json = data.toJSON().collect()
        for d in data_json:
            # sendo to elasticsearch with d as float
            es.index(index="predictions", body=d)

            #print(d, type(d))
    if (choose == 2):
        data_json = data.toJSON().collect()
        for d in data_json:
            es.index(index="lastlaptimes", body=d)



def updateLapTimeTotal_df(df : DataFrame, epoch_id):
    global pilotDataframes
    for row in df.rdd.collect():
        pilotDataframes[row.PilotNumber] = pilotDataframes[row.PilotNumber].union(df)
        print("Aggiornato dataframe del pilota " + str(row.PilotNumber))
        linearRegression(row.PilotNumber)

#def showBatch(df, epoch_id):
#    #trucate false per vedere tutto il contenuto della colonna
#    df.show(truncate=False)
def main():
    global pipeline
    spark = SparkSession.builder \
        .appName("SparkF1") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    preparePilotsDataframes()


    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "broker:29092") \
        .option("subscribe", "LiveTimingData") \
        .load()
    

    vectorAssembler = VectorAssembler(inputCols=["Lap"], outputCol="features", handleInvalid="skip")
    lr = LinearRegression(featuresCol="features",
                          regParam=0.01, labelCol="Seconds", maxIter=10)
    pipeline = Pipeline(stages=[vectorAssembler, lr])
    print("Pipeline creata"+str(type(pipeline)))

    # df = (spark.readStream
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
        get_json_object("json", "$.TimingData.PilotNumber").cast(
            IntegerType()).alias("PilotNumber"),
        get_json_object("json", "$.TimingData.NumberOfLaps").cast(
            IntegerType()).alias("Lap"),
        get_json_object("json", "$.TimingData.LastLapTime.Value").alias("LastLapTime"),
        get_json_object("json", "$.@timestamp").alias("timestamp")    

    ).where("Lap is not null and LastLapTime is not null")

    #laptime_query = laptime_df.writeStream\
    #    .outputMode("append")\
    #    .foreachBatch(updateLapTimeTotal_df)\
    #    .start()



    laptime_query = laptime_df.writeStream.outputMode("append").foreachBatch(updateLapTimeTotal_df).start()
    laptime_query.awaitTermination()

if __name__ == "__main__":
    main()
