import websockets.client as wsc#
import asyncio
import requests as req
import urllib.parse as encoder
import json
import time
import requests as req
import socket

def toSeconds(jsonData):
    if(str(jsonData).find("LastLapTime") != -1):
        try:
        #converti da minuti e secondi e milliosecondi a float
            time1=str(jsonData["LastLapTime"]["Value"])
            time1=int(time1.split(":")[0])*60+float(time1.split(":")[1])
            jsonData["LastLapTime"]["Value"]=time1
        except:
            pass
    return jsonData   
    

def testLogstash():
    while True:
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect(('logstash', 5000))
            sock.close()
            break
        except:
            print("Logstash not ready")
            time.sleep(5)
            continue
def sendToLogstash3(data):
    data = json.dumps(data)
    data = data.encode('utf-8')
    #print(data)
    #print("inviato 3")
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect(('logstash', 5002))
    sock.sendall(data)
    sock.close() 

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
    if(str(data).find("LastLapTime")!=-1):
        data = json.dumps(data, indent=None).encode('utf-8')
        #print(data)
        #print("inviato")
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect(('logstash', 5000))
        sock.sendall(data)
        sock.close()


def jsonModifier(jsonData, pilotsNumber,recapBool):
    if(recapBool):
        jsonData = jsonData["R"]["TimingData"]["Lines"][str(pilotsNumber)]
        jsonData["PilotNumber"]=pilotsNumber
    else:
        jsonData = jsonData["M"][0]["A"][1]["Lines"][str(pilotsNumber)]
        jsonData["PilotNumber"]=pilotsNumber
        jsonData = toSeconds(jsonData)
    return jsonData

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

#def checkRaceControlMessages(data):
#    if data["M"][0]["A"][0] == "RaceControlMessages":
#        sendToLogstash2(data["M"][0]["A"][1]["Messages"])
#        return True
#    else:
#        return False
    
def checkGapTimeData(data):
    data = str(data["M"][0]["A"][1])
    if data.find("GapToLeader") != -1:
        return True
    else:
        return False            

def sender(data, pilotsNumbers):
    try:
        if checkWeather(data):
            return
        #elif checkRaceControlMessages(data):
        #    return
        keys = dict(data["M"][0]["A"][1]["Lines"]).keys()
        keys = str(keys)
    except:
        keys = ""
    # print(data)
    dataString = str(data)
    for pilotNumber in pilotsNumbers:
        if dataString.find("'R'") == -1 and keys.find("'"+str(pilotNumber)+"'") != -1 :
            pilotinfo=jsonModifier(data,pilotsNumber=pilotNumber,recapBool=False)
            if checkGapTimeData(data):

                sendToLogstash3(pilotinfo)
            #print("mando  " + str(pilotinfo))
            else:
                sendToLogstash(pilotinfo)
        elif dataString.find("'R'") != -1:
            pilotinfo=jsonModifier(data,pilotsNumber=pilotNumber,recapBool=True)
            #print("mando  " + str(pilotinfo))
            sendToLogstash(pilotinfo)


def producer(jsondata, pilotsNumbers):
    if jsondata.find("{") != -1:
            jsond = json.loads(jsondata)
            boolean = (str(jsond)).find("'R'")

            if boolean == -1 and jsond["M"].__len__()!= 0 :
                sender(jsond, pilotsNumbers)

            else:
                sender(jsond,pilotsNumbers)



##############################################################################################################
async def negotiate():
    hub= encoder.quote(json.dumps([{"name":"Streaming"}]))
    url="https://livetiming.formula1.com/signalr/negotiate?connectionData="+hub+"&clientProtocol=1.5"
    eventLoop=asyncio.get_event_loop()
    future=eventLoop.run_in_executor(None,req.get,url)
    response=await future
    
    return response


async def sockR(url,cookie):
    header={
			"User-Agent": 'BestHTTP',
			"Accept-Encoding": 'gzip,identity',
			"Cookie": cookie
		    }
    
    connection=await wsc.connect(url,extra_headers=header)

    return connection


async def connectwebs(token, cookie):
    hub= encoder.quote(json.dumps([{"name":"Streaming"}]))
    encodedToken = encoder.quote(token)
    url="wss://livetiming.formula1.com/signalr/connect?clientProtocol=1.5&transport=webSockets&connectionToken="+encodedToken+"&connectionData="+hub
    print(url)
    connection=await sockR(url,cookie)
    
    subscription={
				"H": "Streaming",
				"M": "Subscribe",
				"A": [["WeatherData","TimingData"]],
				"I": 2
			}
    await connection.send(json.dumps(subscription))
    asyncio.create_task(receivingLoop(connection))


async def main():
    file= open("rawData.json","w")
    file.close()
    
    testLogstash()

    response1=await negotiate()
    token=json.loads(response1.text)['ConnectionToken']
    print("TOKEN "+token)
    cookie=response1.headers["Set-Cookie"]
    print("COOKIES "+cookie)
    print(response1.headers)
    
    task1= asyncio.create_task(waitSock(token,cookie))
    task2=asyncio.create_task(wait("check on"))
    await task1
    await task2


async def waitSock(token, cookie):
    sock=await connectwebs(token,cookie)


async def wait(id):
    while True:
        await asyncio.sleep(20)
        print("still on  "+id)

async def receivingLoop(connection):
    pilotsNumbers = getPilotsData()

    while connection.open:
        data=await connection.recv()
        
        if (data != "{}"):
                with open("rawSpain.json","a") as file:
                    file.write(data)
                    file.write("\n")
                    file.flush()
                producer(data,pilotsNumbers)
                #print(data) 
                
                
            
if __name__ == "__main__":
    try:
        asyncio.run(main())     
    except KeyboardInterrupt:
        print("Programma terminato")
        