
# coding: utf-8

# In[1]:


import requests
import json
from pyspark import SparkConf, SparkContext
from elasticsearch import Elasticsearch
from mpl_toolkits.basemap import Basemap
import matplotlib.pyplot as plt
import sys

conf = SparkConf()
conf.setAppName('FlightAnalysis')
conf.setMaster('local')
sc = SparkContext(conf=conf)
NUM_ITERATIONS = 10


def getLocation (loc_string):
    parameters = {"address" : '+'.join(loc_string.split(' ')), "key": "AIzaSyBAk3UD0kxYwvJEAof-3RrY57f2KLKW7mc"}
    response = requests.get("https://maps.googleapis.com/maps/api/geocode/json", params=parameters)
    if response.status_code==200:
        response = json.loads(response.content.decode("utf-8"))
        if response["status"] == "OK":
            results = response["results"]
            dict = results[0]["geometry"]["location"]
    if dict!=None:
        return [dict["lng"], dict["lat"]]


def pageRank(data):
  airportData = data.groupByKey().mapValues(lambda x : list(x))
  airportData1 = airportData.flatMap(lambda x: map(lambda y: (y, []), x[1]))
  airportData = (airportData + airportData1).reduceByKey(lambda x, y: x+y)

  airportDummy = airportData.map(lambda x: (x[0], 0.0))
  airportRanks = airportData.map(lambda x: (x[0], 1.0))

  for i in range(0, NUM_ITERATIONS):
      joinedData = airportData.join(airportRanks)
      airportRanks = joinedData.flatMap(lambda x: map(lambda y: (y, x[1][1]/len(x[1][0])), x[1][0])).reduceByKey(lambda x,y: x+y).mapValues(lambda x: 0.15+ 0.85*x)
      airportRanks = (airportRanks + airportDummy).reduceByKey(lambda x,y: x+y)

  return airportRanks

def clean_data(x):
  id = int(x[0].encode('utf-8'))
  pagerank = x[1]
  loc1 = x[2].replace('"','').split(",",1)
  city = loc1[0]
  loc2 = loc1[1].split(":",1)
  state = loc2[0].replace(' ','')
  airport = loc2[1]
  geopoints = x[3]
  return(city,state,airport,geopoints,pagerank)

#Connectivity Data
def connectivityRanks(data):
    incoming_data = data.map(lambda x: (x[4], x[5]))
    airportRanksIncoming = pageRank(incoming_data)
    outgoing_data = data.map(lambda x: (x[5], x[4]))
    airportRanksOutgoing = pageRank(outgoing_data)
    connectivity = airportRanksIncoming#.join(airportRanksOutgoing).map(lambda x : (x[0], x[1][0] + x[1][0]))
    top10 = connectivity.takeOrdered(10, key = lambda x: -x[1])
    top10_metadata = sc.parallelize(top10).join(metadata)
    finalOutput = top10_metadata.map(lambda x : (x[0], x[1][0], x[1][1], getLocation(x[1][1]))).map(lambda x: clean_data(x))
    return finalOutput


#Cancelled Data
def cancelledRanks(data):
    data_cancelation = data.filter(lambda x: float(x[12])==1)
    data_cancelation = data_cancelation.map(lambda x: (x[4], x[5]))
    airportRankscancellation = pageRank(data_cancelation)
    toptencancellation = airportRankscancellation.takeOrdered(10, key = lambda x: -x[1])
    toptencancellation_metadata = sc.parallelize(toptencancellation).join(metadata)
    finalOutput = toptencancellation_metadata.map(lambda x : (x[0], x[1][0], x[1][1], getLocation(x[1][1]))).map(lambda x: clean_data(x))
    return finalOutput
    

def createDelayLink(x):
    dep = float(x[8])
    arr = float(x[11])
    if dep>0 and ((arr-dep)>0):
        return [(x[4],x[5]),(x[5],x[4])]
    elif dep>0:
        return [(x[5],x[4])]
    elif arr>0:
        return [(x[4],x[5])]

#Delayed Data
def delayFlightRanks(data):
    data = data.filter(lambda x: x[8]!='' and x[11]!='').filter(lambda x: float(x[8])>0 or float(x[11])>0).flatMap(lambda x: createDelayLink(x))
    airportRanks = pageRank(data)
    top10 = airportRanks.takeOrdered(10, key = lambda x: -x[1])
    top10_metadata = sc.parallelize(top10).join(metadata)
    finalOutput = top10_metadata.map(lambda x : (x[0], x[1][0], x[1][1], getLocation(x[1][1]))).map(lambda x: clean_data(x))
    return finalOutput


# Get the hash tag from command line
if len(sys.argv) != 2:
    print ("Invalid command: FlightAnalysis.py [-conn|-cancel|-delay]")
    print ("-conn : Connectivity Analysis")
    print ("-cancel : Cancelled Flight Analysis")
    print ("-delay : Delayed Flight Analysis")
    sys.exit(0)

algorithm = sys.argv[1]

#Fetch flight data from source
metadata = sc.textFile("hdfs://localhost:9000/bigdataproject/data/L_AIRPORT_ID.csv").map(lambda line: line.split("\t"))
data = sc.textFile("hdfs://localhost:9000/bigdataproject/data/Data_Jan_Dec_2017/January2017/*").map(lambda x: x.replace('"', '')).map(lambda line: line.split(","))

data = data.filter(lambda x: x[0]!='FL_DATE')

if algorithm=='-conn':
    finalOutput = connectivityRanks(data)
elif algorithm=='-cancel':
    finalOutput = cancelledRanks(data)
elif algorithm=='-delay':
    finalOutput = delayFlightRanks(data)
else:
    print ("Invalid command: FlightAnalysis.py [-conn|-cancel|-delay]")
    print ("-conn : Connectivity Analysis")
    print ("-cancel : Cancelled Flight Analysis")
    print ("-delay : Delayed Flight Analysis")
    sys.exit(0)


# setup Lambert Conformal basemap.
map = Basemap(width=12000000,height=9000000,projection='lcc',
        resolution=None,lat_1=45.,lat_2=55,lat_0=50,lon_0=-107)
map.shadedrelief()

#Plot points on map
x,y = map(finalOutput.map(lambda x: x[3][0]).collect(), finalOutput.map(lambda x: x[3][1]).collect())
labels = finalOutput.map(lambda x: x[2]).collect()
data = finalOutput.map(lambda x: x[4]).collect()

for label, xpt, ypt in zip(labels, x, y):
    plt.text(xpt, ypt, label.split(',')[0])
cs = map.scatter(x,y,c=data, cmap='jet')
cbar = map.colorbar(cs,location='bottom',pad="5%")

plt.show()







