from dateutil import parser
import time
import json
import requests as req
import socket

def sendToLogstash2(data):
    data = json.dumps(data)
    data = data.encode('utf-8')
    #print(data)
    #print("inviato 2")
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect(('logstash', 5001))
    sock.sendall(data)
    sock.close()

def sendToLogstash(data):
    data = json.dumps(data).encode('utf-8')
    #print(data)
    #print("inviato")
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect(('logstash', 5000))
    sock.sendall(data)
    sock.close()


def jsonModifier(jsonData, pilotsNumber,recapBool):
    if(recapBool):
        jsonData = jsonData["R"]["TimingData"]["Lines"][str(pilotsNumber)]
        jsonData["PilotNumber"]=str(pilotsNumber)
    else:
        jsonData = jsonData["M"][0]["A"][1]["Lines"][str(pilotsNumber)]
        jsonData["PilotNumber"]=str(pilotsNumber)
        
    return jsonData

def timedSender(jsondata, pilotsNumbers, deltaTime):
    if deltaTime > 10 or deltaTime <= 0:
        deltaTime = 0.1
    elif deltaTime > 0: 
        time.sleep(float(deltaTime))
        sender(jsondata, pilotsNumbers)    


def roundFloat(timeOfJson_str):
    actualTime = ISOToFloat(timeOfJson_str)
    decimal = round(actualTime, 3)
    limitedDecimal = "{:.3f}".format(decimal)
    actualTime = float(limitedDecimal)
    return actualTime


def readJsonFile():
    filejson = open("rawDataMonaco.json", "r")
    jsondata = filejson.read()
    filejson.close()  # funzione per preparare il file
    #togli gli spazi tra } e {

    jsondata= jsondata.strip
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




def checkWeather(data):
    if data["M"][0]["A"][0] == "WeatherData":
        sendToLogstash2(data["M"][0]["A"][1])
        return True
    else:
        return False

def checkRaceControlMessages(data):
    if data["M"][0]["A"][0] == "RaceControlMessages":
        sendToLogstash2(data["M"][0]["A"][1]["Messages"])
        return True
    else:
        return False
    
            

def sender(data, pilotsNumbers):
    try:
        if checkWeather(data):
            return
        elif checkRaceControlMessages(data):
            return
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
            sendToLogstash(pilotinfo)
        elif dataString.find("'R'") != -1:
            #pilotinfo = data["R"]["TimingData"]["Lines"][str(pilotNumber)]
            pilotinfo=jsonModifier(data,pilotsNumber=pilotNumber,recapBool=True)
            #print("mando  " + str(pilotinfo))
            sendToLogstash(pilotinfo)



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
    pilotsNumbers = getPilotsData()
    #pilotsNumbers = [1, 16, 55, 4, 10, 11, 14, 18, 20, 22, 23, 24, 27, 31, 44, 63, 77, 81, 2, 21]

    time.sleep(30)
    previousTime = -40
    deltaTime = 0

    filejson = open("rawDataMonaco.json", "r")
    for jsondata in filejson:
        
        if jsondata.find("{") != -1:
            jsond = json.loads(jsondata)
            boolean = (str(jsond)).find("'R'")

            if boolean == -1 and jsond["M"].__len__()!= 0 :
                actualTime = roundFloat(jsond["M"][0]["A"][2])
                if previousTime == -40:
                    previousTime = actualTime
                # delta tra precedente e attuale
                deltaTime = actualTime-previousTime
                previousTime = actualTime
                timedSender(jsond, pilotsNumbers, deltaTime)

            else:
                timedSender(jsond,pilotsNumbers, 0.001)

    print("End")
    filejson.close()


def main():
    try:
        Start()
    except KeyboardInterrupt:
        print("KeyboardInterrupt has been caught.")
        exit()    


if __name__ == "__main__":
    main()
