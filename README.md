# Spark Streaming to get 'mentioned' twitter user IDs
A spark application that emits the twitter IDs of users that have been mentioned by other users every 10 seconds.
Trigger method on writeStream object was used to specify the processingTime as 10 seconds.
A select query was used to obtain the userIDs from the input stream and a where clause was specified to filter interactions that were 'MT's.

## Dataset
This application was developed to analyze the Higgs Twitter Dataset. The Higgs dataset has been built after monitoring the spreading processes on Twitter before, during and after the announcement of the discovery of a new particle with the features of the Higgs boson. Each row in this dataset is of the format <userA, userB, timestamp, interaction> where interactions can be retweets (RT), mention (MT) and reply (RE).
We have split the dataset into a number of small files so that we can use the dataset to emulate streaming data. Download the split dataset onto your master VM.

## streamer.sh
This script emulates twitter stream by doing the following :
* Copies the entire split dataset to the HDFS. This would be the staging directory.
* Creates a monitoring directory on the HDFS that this application listens to. This would be the directory this streaming application is listening to.
* Periodically, moves the split dataset files from the staging directory to the monitoring directory using the hadoop fs -mv command.

## Usage
Submit this spark job by using the following command : 

`spark-submit --verbose usermentions.py <path_to_monitoring_dir_in_hdfs>`

The output path for the parquet file  is hard coded to be '/user/ubuntu/b2_output'

