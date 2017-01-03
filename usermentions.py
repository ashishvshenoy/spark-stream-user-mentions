from __future__ import print_function

import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split
from pyspark.sql.functions import window
from pyspark.sql.types import StructType
from pyspark.sql.streaming import DataStreamReader

if __name__ == "__main__":
    if len(sys.argv) != 2 :
        msg = ("Usage: b1_tweetcount.py <monitoring_dir>")
        print(msg, file=sys.stderr)
        exit(-1)

    windowSize = "3600"
    slideSize = "1800"
    if slideSize > windowSize:
        print("<slide duration> must be less than or equal to <window duration>", file=sys.stderr)
    windowDuration = '{} seconds'.format(windowSize)
    slideDuration = '{} seconds'.format(slideSize)
    monitoring_dir = sys.argv[1]

    spark = SparkSession\
        .builder\
        .appName("MentionedUsers")\
        .config("spark.driver.memory","1g")\
        .config("spark.eventLog.enabled","true")\
        .config("spark.eventLog.dir","hdfs://10.254.0.33:8020/user/ubuntu/applicationHistory")\
        .config("spark.executor.memory","1g")\
        .config("spark.executor.cores","4")\
        .config("spark.task.cpus","1")\
        .config("spark.executor.instances","5")\
        .master("spark://10.254.0.33:7077")\
        .getOrCreate()

    userSchema = StructType().add("userA","string").add("userB","string").add("timestamp","timestamp").add("interaction","string")
    csvDF = spark\
        .readStream \
	.schema(userSchema) \
	.csv(monitoring_dir)

    mentions = csvDF.select(csvDF['userB']).where("interaction LIKE 'MT'");
    query = mentions\
        .writeStream\
	.format('parquet')\
        .trigger(processingTime='10 seconds')\
	.outputMode("append")\
	.queryName("test")\
	.option("checkpointLocation","/user/ubuntu/b2_output")\
        .start(path="/user/ubuntu/b2_output")

    
    query.awaitTermination()
