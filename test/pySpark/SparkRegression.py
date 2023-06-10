from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType, StringType
from pyspark.sql import SparkSession
from pyspark.ml.regression import LinearRegression
from pyspark.ml.feature import VectorAssembler
from pyspark.ml import Pipeline
import requests as req
import json
import elasticsearch
import pandas as pd


es = elasticsearch.Elasticsearch(hosts=["http://elasticsearch:9200"])
pilotDataframes = {}
pilotModels = {}
pipeline=None

laptime_schema = StructType([
    StructField("PilotNumber", IntegerType(), True),
    StructField("Lap", IntegerType(), True),
    StructField("Seconds", FloatType(), True),
    StructField("@timestamp", StringType(), True)
])
prediction_schema = StructType([
    StructField("PilotN", IntegerType(), True),
    StructField("Lap", IntegerType(), True)
])
#crea un dict di modelli di MLlib vuoti per ogni pilota
def preparePilotModels():
    pilots=getPilotsData()
    global pilotModels
    for pilot in pilots:
        pilotModels[pilot] = 0
#applica la regressione lineare ai tempi del pilota pilotNumber e invia i risultati a ES
#la regressione viene applicata ogni 5 giri tranne i primi 5
# inoltre viene applicata se il modello non è stato ancora creato
# aggiunge anche il timestamp  
def linearRegression(pilotNumber):
    global pilotDataframes
    global pipeline
    global pilotModels
    
    df = pilotDataframes[pilotNumber]
    next_lap = df["Lap"].max() + 1
    spark_session = SparkSession.builder.appName("SparkF1").getOrCreate()
    print(df)
    if(int(next_lap)%2==0 or pilotModels[pilotNumber]==0 or int(next_lap)<6):
        df= spark_session.createDataFrame(df, laptime_schema)
        pilotModels[pilotNumber] = pipeline.fit(df)   
        df.unpersist()

    model = pilotModels[pilotNumber]
    NextLap_df = spark_session.createDataFrame([(pilotNumber, next_lap)], prediction_schema).cache()
    predictions = model.transform(NextLap_df).withColumn("prediction", col("prediction").cast(FloatType())).cache()
    NextLap_df.unpersist()
    predictions = predictions.withColumnRenamed("Lap", "NextLap").withColumn("@timestamp", current_timestamp())
    predictions.show()
    sendToES(predictions, 1)
        
#crea una lista di piloti
#viene sostituito il numero del 33 (Verstappen) con 1 (perchè db non aggiornato bene)
def makePilotList(pilotsRaw):

    pilots = []
    for i in range(20):
        if pilotsRaw[i]["permanentNumber"] == "33":
            pilots.append(1)
        else:
            pilots.append(int(pilotsRaw[i]["permanentNumber"]))
    return pilots

#ottiene i dati dei piloti dal db
def getPilotsData():
    dbUrl = "http://ergast.com/api/f1/2023/drivers.json"
    PilotData = req.get(dbUrl)
    PilotJson = (json.loads(PilotData.text))[
        "MRData"]["DriverTable"]["Drivers"]
    pilotList = makePilotList(PilotJson)

    return pilotList

#crea un dict di dataframe vuoti per ogni pilota
def preparePilotsDataframes():
    pilotList = getPilotsData()
    global pilotDataframes
    for i in range(20):
        pilotDataframes[pilotList[i]] = pd.DataFrame(
            columns=["PilotNumber", "Lap", "Seconds"])

 #invia i dati a elasticsearch in formato json in base al parametro choose
 #choose=1 invia i dati di predizione
 #choose=2 invia i dati di lastlaptime
def sendToES(data: pd.DataFrame, choose: int):
    global es
    if (choose == 1):
        data_json = data.toJSON().collect()
        data.unpersist()
        #print(data_json)
        for d in data_json:
            # sendo to elasticsearch with d as float
            es.index(index="predictions", document=d)
    if (choose == 2):
        #in questo caso data è un dataframe pandas
        #converto in json e invio
        data_json = data.to_json(orient="records", default_handler=list)
        print(data_json[0], type(data_json))
        #es.index(index="lastlaptimes", document=data_json[0])
    

#aggiorna l'ultimo giro dei piloti man mano che questi completano un giro
#viene in particolare aggiornato il dict dei dataframe con gli ultimi 5 giri e messo in cache
#la cache permette di risolvere i problemi di performance dovuti al fatto che i dataframe vengono
#ricreati ad ogni batch

def updateLapTimeTotal_df(df: DataFrame, epoch_id):
    global pilotDataframes
    if df.count() > 0:
        pandas_df=df.toPandas()    
        print("New batch arrived")
        
        for _, row in pandas_df.iterrows():
            pn = row['PilotNumber']
            old_df = pilotDataframes[pn]
            df2 = pandas_df[pandas_df['PilotNumber'] == pn]
            pilotDataframes[pn] = pd.concat([old_df, df2]).sort_values('Lap', ascending=False).head(10).copy()
            sendToES(df2, 2)
            linearRegression(pn)
        

def main():
    global pipeline


    

    spark = SparkSession.builder \
        .appName("SparkF1") \
        .config("spark.sql.shuffle.partitions", "8")  \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    preparePilotsDataframes()
    preparePilotModels()

    vectorAssembler = VectorAssembler(inputCols=["Lap"], outputCol="features", handleInvalid="skip")
    lr = LinearRegression(featuresCol="features",
                          regParam=0.01, labelCol="Seconds", maxIter=6)
    pipeline = Pipeline(stages=[vectorAssembler, lr])
    print("Pipeline creata"+str(type(pipeline)))

    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "broker:29092") \
        .option("maxOffsetsPerTrigger", "10") \
        .option("subscribe", "LiveTimingData") \
        .option("startingOffsets", "latest") \
        .load()


    #opzione per leggere da kafka da confluent cloud
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

    df2 = df.select(col("value").cast("string").alias("json")).select(
        get_json_object("json", "$.TimingData.PilotNumber").cast(
            IntegerType()).alias("PilotNumber"),
        get_json_object("json", "$.TimingData.NumberOfLaps").cast(
            IntegerType()).alias("Lap"),
        get_json_object("json", "$.TimingData.LastLapTime.Value").cast(
            FloatType()).alias("Seconds"),
        get_json_object("json", "$.@timestamp").alias("@timestamp")   

    ).where("Lap is not null and Seconds is not null")
    laptime_query = df2.writeStream\
        .outputMode("append").foreachBatch(updateLapTimeTotal_df).start()
    laptime_query.awaitTermination()

if __name__ == "__main__":
    main()
