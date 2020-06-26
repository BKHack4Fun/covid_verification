"""
Module process data with pyspark
"""

import json

from pyspark import SparkContext
from pyspark.streaming import StreamingContext

# Create a local StreamingContext with two working thread and batch interval of 1 second
from db_services.neo4j_services import update_graph
from nlp.ERextractor import ER_extractor

sc = SparkContext("local[*]", "StreamingCovid19")
ssc = StreamingContext(sc, 30)
# Create a DStream that will connect to hostname:port, like localhost:9999
# lines = ssc.socketTextStream("localhost", 9999)


lines = ssc.textFileStream("E:/HUST/bigdata/covid_verification/dataprocessing")


def extractER(line):
    js = json.loads(line)
    print(js['content'])
    update_graph(js['content'], js['time'], js["link"])
    return ("add doc", 1)


words = lines.flatMap(extractER)
words.pprint()
ssc.start()  # Start the computation
ssc.awaitTermination()  # Wait for the computation to terminate
