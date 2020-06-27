"""
Module process data with pyspark
"""

import json

import pyspark as ps
from pyspark.streaming import StreamingContext

# Create a local StreamingContext with two working thread and batch interval of 1 second
from db_services.neo4j_services import update_graph
from nlp.ERextractor import ER_extractor

conf = ps.SparkConf().setMaster("local[*]").setAppName("StreamingCovid19")
conf.set("spark.executor.heartbeatInterval", "3600s")
conf.set("spark.network.timeout", "3700s")
conf.set("spark.storage.blockManagerSlaveTimeoutMs", "3700s")
sc = ps.SparkContext('local[*]', '', conf=conf)
ssc = StreamingContext(sc, 30)
# Create a DStream that will connect to hostname:port, like localhost:9999
# lines = ssc.socketTextStream("localhost", 9999)


lines = ssc.textFileStream("E:/HUST/bigdata/covid_verification/dataprocessing/data")


def extractER(line):
    js = json.loads(line)
    print(js['content'])
    update_graph(js['content'], js['time'], js["link"])
    return ("add doc", 1)


words = lines.flatMap(extractER)
words.pprint()
ssc.start()  # Start the computation
ssc.awaitTermination()  # Wait for the computation to terminate
