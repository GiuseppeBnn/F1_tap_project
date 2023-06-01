from dateutil import parser
import time
import json
from kafka import KafkaProducer
import requests as req


def jsonModifier(jsonData, pilotsNumber,recapBool):
    if(recapBool):
        jsonData = jsonData["R"]["TimingData"]["Lines"][str(pilotsNumber)]
        jsonData["PilotNumber"]=str(pilotsNumber)
    else:
        jsonData = jsonData["M"][0]["A"][1]["Lines"][str(pilotsNumber)]
        jsonData["PilotNumber"]=str(pilotsNumber)
        
    return jsonData

def timedSender(jsondata, producer, pilotsNumbers, deltaTime):
    if deltaTime > 10 or deltaTime <= 0:
        deltaTime = 0.1
    elif deltaTime > 0: 
        time.sleep(float(deltaTime))
        sendToKafka(jsondata, producer, pilotsNumbers)    


def roundFloat(timeOfJson_str):
    actualTime = ISOToFloat(timeOfJson_str)
    decimal = round(actualTime, 3)
    limitedDecimal = "{:.3f}".format(decimal)
    actualTime = float(limitedDecimal)
    return actualTime


def readJsonFile():
    filejson = open("rawMiami.json", "r")
    jsondata = filejson.read()
    filejson.close()  # funzione per preparare il file
    jsondata = jsondata.replace("}{", "}£{")
    jsondatalist = jsondata.split("£")
    return jsondatalist


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


def sendToKafka(data, producer, pilotsNumbers):

    #data = json.loads(data)
    try:
        keys = dict(data["M"][0]["A"][1]["Lines"]).keys()
        keys = str(keys)
    except:
        keys = ""
    # print(data)
    dataString = str(data)
    for pilotNumber in pilotsNumbers:
        if dataString.find("'R'") == -1 and keys.find("'"+str(pilotNumber)+"'") != -1 :
            #pilotinfo = data["M"][0]["A"][1]["Lines"][str(pilotNumber)]
            pilotinfo=jsonModifier(data,pilotsNumber=pilotNumber,recapBool=False)
            #print("mando  " + str(pilotinfo))
            producer.send("LiveTimingData", pilotinfo)
            producer.flush()
        elif dataString.find("'R'") != -1:
            #pilotinfo = data["R"]["TimingData"]["Lines"][str(pilotNumber)]
            pilotinfo=jsonModifier(data,pilotsNumber=pilotNumber,recapBool=True)
            #print("mando  " + str(pilotinfo))
            producer.send("LiveTimingData", pilotinfo)
            producer.flush()



def ISOToFloat(datestring):
    yourdate = parser.parse(datestring)
    splitDate = str(yourdate).split(' ')[1].split('+')[0].split(':')
    year = splitDate[0]
    month = splitDate[1]
    day = splitDate[2]
    floatDate = float(year+month+day)
    return floatDate


def Start():
    print("Start")
    jsondatalist = readJsonFile()
    #pilotsNumbers = getPilotsData()
    pilotsNumbers = [1, 16, 55, 4, 10, 11, 14, 18, 20, 22, 23, 24, 27, 31, 44, 63, 77, 81, 2, 21]
    producer = KafkaProducer(bootstrap_servers='broker:29092',
                             value_serializer=lambda v: json.dumps(v).encode('utf-8'))
    previousTime = -40
    deltaTime = 0

    for jsondata in jsondatalist:
        jsond = json.loads(jsondata)
        boolean = (str(jsond)).find("'R'")

        if boolean == -1:
            actualTime = roundFloat(jsond["M"][0]["A"][2])
            if previousTime == -40:
                previousTime = actualTime
                # delta tra precedente e attuale
            deltaTime = actualTime-previousTime
            previousTime = actualTime
            timedSender(jsond, producer, pilotsNumbers, deltaTime)

        else:
            timedSender(jsond, producer, pilotsNumbers, 0.001)

    print("End")


def main():
    try:
        Start()
    except KeyboardInterrupt:
        print("KeyboardInterrupt has been caught.")
        exit()    


if __name__ == "__main__":
    main()
