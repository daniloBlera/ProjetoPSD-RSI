# -*- coding: utf-8 -*-
from datetime import datetime

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.mqtt import MQTTUtils
import pika


sc = SparkContext("local[5]", "Jesus Christ that's Jason Bourne")
ssc = StreamingContext(sc, 1)
ssc.checkpoint("/tmp/spark-streaming-checkpoints")

parameters = pika.ConnectionParameters(host='localhost', port=5672)
connection = pika.BlockingConnection(parameters)
channel = connection.channel()
broker_url = 'tcp://localhost:1883'
exchange_name = "amq.topic"
queue_name = "SPARK_POST_STRUCTURES"

post_structures = MQTTUtils.createStream(ssc, broker_url, queue_name)
sep = ">>"


def push_scores_to_queue(time, rdd):
    print("======{}======".format(time))
    elements = None

    if rdd.isEmpty():
        print("-EMPTY-")
    else:
        elements = rdd.map(lambda pair: (str(pair[0]), str(pair[1]))).collect()

        content = []
        for e in elements:
            print(e)
            content.append(','.join(e))

        elements = '>>'.join(content)

    parameters = pika.ConnectionParameters(host='localhost', port=5672)
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()
    exchange_name = "amq.topic"
    queue_name = "SPARK_PROCESSING_RESPONSE"
    channel.exchange_declare(exchange=exchange_name, type='topic', durable=True)

    channel.basic_publish(
        exchange=exchange_name,
        routing_key=queue_name,
        body=str(elements)
    )

    channel.close()

def get_days_difference_between(timestamp1, timestamp2):
    ts_fmt = "%Y-%m-%dT%H:%M:%S.%f+0000"

    t1 = datetime.strptime(timestamp1, ts_fmt)
    t2 = datetime.strptime(timestamp2, ts_fmt)

    return (t1 - t2).days


def update_event(new_event, last_event):
    if new_event:
        return new_event[0]

    return last_event

if __name__ == "__main__":
    print("Processamento de pontuações iniciado")

    active_posts = post_structures.map(
        lambda evt: (evt.split(sep)[1].split('|')[1] + sep + evt.split(sep)[0],
                     evt.split(sep)[1:])).flatMapValues(
        lambda evt: evt).map(
        lambda pair: (pair[0].split(sep)[0],
                     (pair[0].split(sep)[1], pair[1].split('|')[0]) )
        ).mapValues(
        lambda pair: 10 - get_days_difference_between(pair[0], pair[1])
        ).reduceByKey(lambda x, y: x+y)

    active_posts.foreachRDD(push_scores_to_queue)

    ssc.start()
    ssc.awaitTermination()
    ssc.stop()

