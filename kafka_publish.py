from confluent_kafka import Producer
from queue import Queue
from threading import Thread
import sys
from time import sleep

BROKER = "127.0.0.1"
TOPIC = "sampleTopic"


def delivery_callback(err, msg):
    if err:
        sys.stderr.write('%% Message failed delivery: %s\n' % err)
    else:
        sys.stdout.write('%% Message delivered to %s [%d] @ %d\n' %
                            (msg.topic(), msg.partition(), msg.offset()))

def connect_to_broker():
    configuration = {'bootstrap.servers':BROKER}
    kafka_producer = Producer(**configuration)
    print("Kafka producer connected to Broker")
    return kafka_producer

def publish_loop(msg_queue):
    producer = connect_to_broker()
    while True:
        try:   
            msg = msg_queue.get()     
            producer.produce(TOPIC, msg, callback = delivery_callback)
        except Exception as e:
            sys.stderr.write("Error: " + str(e))
        finally:    
            producer.flush()

def dummy_msg_producer(msg_queue):
        counter = 0
        while True:
            sleep(3)
            msg = "message" + str(counter)
            msg_queue.put(msg)
            counter += 1

if __name__ == "__main__":
    msg_queue = Queue()
    t_kafka = Thread(target=publish_loop, args=(msg_queue,))
    # For testing
    # t_msg_stream = Thread(target=dummy_msg_producer, args=(msg_queue,)) 

    t_kafka.start()
    # For testing
    # t_msg_stream.start()