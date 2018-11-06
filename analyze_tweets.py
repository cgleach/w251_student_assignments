from pyspark.sql import SparkSession
from pyspark.sql.types import StringType
from pyspark.sql.functions import udf
from pyspark.sql import Row
from pyspark.sql import SQLContext
from pyspark.streaming import StreamingContext
import tweepy
import json


def groupByHash(current_state, new_values):
    """
    Function which returns a new tuple by incrementing curr_count (count always =1),
    adding the new user to the full list of users of this hashtag (key), and adding
    all associated mentioned to the larger list of mentions associated with this hashtag (key)
    """
    (count, user, mentions) = new_values
    (curr_count, curr_users, curr_mentions) = current_state
    return (curr_count+count, curr_users + user, curr_mentions + mentions)


def mapLine(x):
    """
    Splits each line into a user, hashtag, and list of mentions.  If there are
    mentions associated with hashtag the list is evaluated, otherwise it is set
    to an empty list.  We return a key value pair, with hashtag as key,
    and a tuple of count, list with user, and list with mentions
    """
    [user, hashtag, mentioned] = x.split("|")
    if len(mentioned) > 0:
        mentioned = eval(mentioned)
        return (hashtag, (1, [user], mentioned))
    else:
        return (hashtag, (1, [user], []))


def aggregate_hashtag_state(new_values, current_state):
    """
    Checks if current key exists and if so sets current values
    to the state; otherwise, initializes values to 0 and empty lists.
    Then goes through and updates the overall state.
    """
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
    """
    For each RDD in streaming process converts key value pairs to rows and
    inserts into a spark DataFrame.  This df is then registered as a temp
    table which we can query to display our results.
    """
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
    # Set up spark session and context
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
    short_window = data.window((stream_length*window_slides),stream_length)   

    new_data = data.flatMap(lambda line: line.split("*new*"))
    newest = new_data.map(mapLine).reduceByKey(groupByHash)
    running_counts = newest.updateStateByKey(aggregate_hashtag_state)
    sorted_running_counts = running_counts.transform(lambda rdd: rdd.sortBy(lambda tag_info: tag_info[1][0], ascending = False))
    sorted_running_counts.foreachRDD(process_rdd)

    short_window = short_window.flatMap(lambda line: line.split("*new*")).map(mapLine).reduceByKey(groupByHash) 
    short_window.foreachRDD(process_rdd)

    ssc.start()
    ssc.awaitTermination()
    

