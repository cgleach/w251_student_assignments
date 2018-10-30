from pyspark.sql import SparkSession
from pyspark.sql.types import StringType
from pyspark.sql.functions import udf
from pyspark.sql import Row
from pyspark.sql import SQLContext
from pyspark.streaming import StreamingContext
import tweepy
import json


def groupByHash(current_state, new_values):
    (count, mentioned_by, mentioned) = new_values
    (n, n_mentioned_by, n_mentioned) = current_state
    return (n+count, n_mentioned_by + mentioned_by, n_mentioned + mentioned)


def mapLine(x):
    [user, hashtag, mentioned] = x.split("|")
    if len(mentioned) > 0:
        mentioned = eval(mentioned)
        return (hashtag, (1, [user], mentioned))
    else:
        return (hashtag, (1, [user], []))


def aggregate_hashtag_state(new_values, current_state):
    if current_state is not None:
        (count,users,mentions) = current_state
    else:
        count, users, mentions = 0, [], []
    
    if new_values is not None:
        for tup in new_values:
            count += tup[0]
            users += tup[1]
            mentions += tup[2]
    return (count,users,mentions)


def process_rdd(time, rdd):
	try:
    	    sql_context = SQLContext(rdd.context)
   	    row_rdd = rdd.map(lambda w: Row(hashtag=w[0], hashtag_count=w[1][0],users=w[1][1],mentions=w[1][2]))
    	    hashtags_df = sql_context.createDataFrame(row_rdd)
    	    hashtags_df.registerTempTable("hashtags")
    	    hashtag_counts_df = sql_context.sql("select * from hashtags order by hashtag_count desc limit 10")
    	    hashtag_counts_df.show()
        except:
            pass


if __name__ == "__main__":
    sc = SparkSession.builder.appName('hw9').getOrCreate()
    s_context = sc.sparkContext

    # Stream length sets length of time stream occurs to create each RDD.  Window slides
    # determines how many rdds ("windows") will be included in our subset view.
    stream_length = 10
    window_slides = 3

    # Create a streaming context and checkpoint in hdfs
    ssc = StreamingContext(s_context,stream_length)
    ssc.checkpoint("hdfs://10.161.127.131:9000/hw9/checkpoints")
  
    # Configure local IP and port and activate stream
    IP = "169.45.88.196"
    Port = 5555 
    data = ssc.socketTextStream(IP, Port)
   

    new_data = data.flatMap(lambda line: line.split("*new*"))
    newest = new_data.map(mapLine).reduceByKey(groupByHash)
    running_counts = newest.updateStateByKey(aggregate_hashtag_state)
    sorted_running_counts = running_counts.transform(lambda rdd: rdd.sortBy(lambda tag_info: tag_info[1][0], ascending = False))
#    sorted_running_counts.foreachRDD(process_rdd)

    short_window = data.window((stream_length*window_slides),stream_length).flatMap(lambda line: line.split("*new*")).map(mapLine).reduceByKey(groupByHash) 
    short_window.foreachRDD(process_rdd)

    ssc.start()
    ssc.awaitTermination()
    

