from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType, TimestampType
from pyspark.sql import SparkSession
from pyspark.ml.regression import LinearRegression
from pyspark.ml.feature import VectorAssembler
from pyspark.ml import Pipeline
import requests as req
import json
import elasticsearch


es = elasticsearch.Elasticsearch(hosts=["http://elasticsearch:9200"])
pilotDataframes = {}
pilotModels = {}
pipeline=None

laptime_schema = StructType([
    StructField("PilotNumber", IntegerType(), True),
    StructField("Lap", IntegerType(), True),
    StructField("Seconds", FloatType(), True),
    StructField("@timestamp", TimestampType(), True)
])
prediction_schema = StructType([
    StructField("PilotN", IntegerType(), True),
    StructField("Lap", IntegerType(), True)
])

def preparePilotModels():
    pilots=getPilotsData()
    global pilotModels
    for pilot in pilots:
        pilotModels[pilot] = 0

def linearRegression(pilotNumber):
    global pilotDataframes
    global pipeline
    global pilotModels
    df = pilotDataframes[pilotNumber]
    if df.count() > 0:
        NextLap = df.limit(1).collect()[0]["Lap"]+1
        #df.show()
        print("Provo a creare il modello del pilota " + str(pilotNumber))
        if(int(NextLap)%5==0 or pilotModels[pilotNumber]==0 or int(NextLap)<4):
            print("riaddestro il modello del pilota " + str(pilotNumber))
            pilotModels[pilotNumber] = pipeline.fit(df)
            print("Modello del pilota " + str(pilotNumber) + " creato")
        

        model = pilotModels[pilotNumber]
        spark_session = SparkSession.builder.appName("SparkF1").getOrCreate()
        print("Provo a fare la predizione del pilota " + str(pilotNumber))
        NextLap_df = spark_session.createDataFrame([(pilotNumber, NextLap)], prediction_schema)
        print("Predizione del pilota " + str(pilotNumber) + " creata")
        predictions = model.transform(NextLap_df)

        predictions = predictions.withColumnRenamed("Lap", "NextLap")

        predictions = predictions.withColumn("@timestamp", current_timestamp())
        #predictions.show()
        print("Provo a inviare la predizione del pilota " + str(pilotNumber))
        sendToES(predictions, 1)
        

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
        


def sendToES(data : DataFrame, choose: int):
    global es
    if (choose == 1):
        data= data.withColumn("NextLap", data["NextLap"].cast(IntegerType()))
        data_json = data.toJSON().collect()
        for d in data_json:
            # sendo to elasticsearch with d as float
            es.index(index="predictions", body=d)

            #print(d, type(d))
    if (choose == 2):
        data=data.withColumn("Seconds", data["Seconds"].cast(FloatType()))
        data_json = data.toJSON().collect()
        for d in data_json:
            es.index(index="lastlaptimes", body=d)



def updateLapTimeTotal_df(df : DataFrame, epoch_id):
    global pilotDataframes
    if df.count() > 0:
        print("New batch arrived")
        df.show()    #added for debug
        for row in df.rdd.collect():
        
            df2=df.filter(df.PilotNumber==row.PilotNumber)
            pilotDataframes[row.PilotNumber] = pilotDataframes[row.PilotNumber].union(df2)
            print("Aggiornato dataframe del pilota " + str(row.PilotNumber))
            #seleziona gli utlimi 5 giri per pilota
            pilotDataframes[row.PilotNumber]=(pilotDataframes[row.PilotNumber].orderBy("Lap", ascending=False).limit(5))
            print("Ridotto dataframe del pilota " + str(row.PilotNumber))
            pilotDataframes[row.PilotNumber]= pilotDataframes[row.PilotNumber].withColumn("Seconds", pilotDataframes[row.PilotNumber]["Seconds"].cast(FloatType()))
            print("Cambiato tipo di Seconds")
            linearRegression(row.PilotNumber)
            sendToES(df2, 2)

def main():
    global pipeline


    spark = SparkSession.builder \
        .appName("SparkF1") \
        .getOrCreate()
    

    spark.sparkContext.setLogLevel("WARN")

    preparePilotsDataframes()
    preparePilotModels()


    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "broker:29092") \
        .option("maxOffsetsPerTrigger", "10") \
        .option("subscribe", "LiveTimingData") \
        .option("startingOffsets", "latest") \
        .load()
    

    vectorAssembler = VectorAssembler(inputCols=["Lap"], outputCol="features", handleInvalid="skip")
    lr = LinearRegression(featuresCol="features",
                          regParam=0.01, labelCol="Seconds", maxIter=6)
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
        get_json_object("json", "$.TimingData.LastLapTime.Value").alias("Seconds"),
        get_json_object("json", "$.@timestamp").alias("@timestamp")    

    ).where("Lap is not null and Seconds is not null")

    #laptime_query = laptime_df.writeStream\
    #    .outputMode("append")\
    #    .foreachBatch(updateLapTimeTotal_df)\
    #    .start()



    laptime_query = laptime_df.writeStream.outputMode("append").foreachBatch(updateLapTimeTotal_df).start()
    laptime_query.awaitTermination()

if __name__ == "__main__":
    main()
