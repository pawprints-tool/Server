#!/usr/bin/env python3
import socket
from csv import writer
from datetime import datetime
import os
import json
import time
from threading import Thread, Event
from queue import Queue
import argparse
import signal

HOST = "127.0.0.1"  # The server's hostname or IP address
DATA_PORT = 12348  # The port at which the Android App sends measurements
KILL_OLD_PROCESS = "fuser -k " + str(DATA_PORT) + "/tcp"
PORT_REVERSE_COMMAND = "adb reverse tcp:" + str(DATA_PORT) + " tcp:" + str(DATA_PORT)
TCP_BUFFER_SIZE = 10000
SOCKET_TIMEOUT = 3 # seconds

Thread_Stop_Flag = Event()

def sig_handler(signum, frame):
    print(signum)
    
    global Thread_Stop_Flag
    Thread_Stop_Flag.set()
   
    signal.signal(signal.SIGINT, sig_handler)    
    signal.signal(signal.SIGTERM, sig_handler)
    signal.signal(signal.SIGHUP, sig_handler)
    signal.signal(signal.SIGQUIT, sig_handler)


def _merge_json_str(partial_json, block_open_count, block_closed_count):
    prev_char = ""
    is_complete = False
    log_line_accumulator = ""
    for char in partial_json:
        if char == "\n":
            continue
        if (char == "{" and prev_char != "," and prev_char != ":" and prev_char != "[" and block_open_count > 0) \
            or (char == "}" and block_open_count == 0) \
            or (block_open_count == 0 and block_closed_count == 0 and char != "{"):
                # A log line was abruptly cut off. Reset to the next line.
                log_line_accumulator = ""
                block_open_count = 0
                block_closed_count = 0
                print("Current char=" + str(char) + ", prev char = " + str(prev_char))
                prev_char = char
                CHAR_EXCEPTION_COUNTER += 1
                continue
        
        log_line_accumulator += char
        if char == "{":
            block_open_count += 1
        elif char == "}":
            block_closed_count += 1

        if (block_open_count == block_closed_count) and block_open_count != 0:
            is_complete = True
            log_line_accumulator = ""

        prev_char = char
        return is_complete
    

def recv_stream_from_phone(options, kafka_publish_queue):
    os.system(KILL_OLD_PROCESS)
    os.system(PORT_REVERSE_COMMAND)
    global Thread_Stop_Flag
    
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind((HOST, DATA_PORT))
        s.listen()
        s.settimeout(options.timeout)
        while not Thread_Stop_Flag.is_set():
            try:
                conn, addr = s.accept()
                partial_json = ""
                block_open_count = 0
                block_closed_count = 0
                while not Thread_Stop_Flag.is_set():
                    data = conn.recv(TCP_BUFFER_SIZE).decode('utf-8')
                    if len(data) == 0:
                        break
                    print(data)
                    partial_json += data
                    is_complete = _merge_json_str(partial_json, block_open_count, block_closed_count)
                    if not is_complete:
                        continue
                    
                    data_obj = json.loads(partial_json)
                    partial_json = ""
                    block_open_count = 0
                    block_closed_count = 0
                    
                    log_prefix = options.logprefix
                    if log_prefix is None:
                        log_prefix = data_obj["campaign_name"]

                    log_type = data_obj["type"] if "type" in data_obj else "cellular"
                    log_path = os.path.join(options.logdir, log_prefix + "_pawprints_" + log_type + ".jsonl")
                        
                    if not os.path.exists(options.logdir):
                        os.makedirs(options.logdir)

                    data_obj["companion_abs_time"] = time.time()*1000.0

                    log_string = json.dumps(data_obj)
                    
                    with open(log_path, "a") as file_object:
                        file_object.write(log_string + "\n")

                    if options.kafka:
                        kafka_publish_queue.put(log_string)

            except socket.timeout:
                continue

            except Exception as e:
                print("Exception while receiving packets:" + str(e))
                continue
            
        print("Closing the companion data socket.")
        s.close()
        
    print("Exiting the socket server loop, to which the phone connects")


def main():
    parser = argparse.ArgumentParser(description='This is the PawPrints companion script, meant to be run on a compute node connected to PawPrints via USB. This script receives measurements from PawPrints Phone App, over ADB and saves locally as JSON files. Different JSON files are created for cellular and motions logs. Optionally, measurements can also be sent to a Kafka broker, to be analyzed and visualized by remote control or monitoring centers. ')
    parser.add_argument('-p', '--logprefix', type=str, help='Optional. Prefix of the log file. prefix_cellular.jsonl and prefix_motion.jsonl logs will be generated. By default, the measurement campaign name, as specified in the measurement stream received from the phone, will be used as the prefix.')
    parser.add_argument('-d', '--logdir', type=str, default="./Logs", help="Optional. Path of the logs directory. Directory will be created if it doesn't exist. By default, logs will be saved under ./Logs.")
    parser.add_argument('-k', '--kafka', action='store_true', help="Optional. Enable publishing measurements to Kafka.")
    parser.add_argument('-t', '--timeout', type=float, default=SOCKET_TIMEOUT, help="Optional. Socket timeout, when waiting for connections from the phone. Default value is 3 seconds")

    options = parser.parse_args()
    print("Options: ")
    print("Publish to Kafka: {}".format(options.kafka))
    print("Log prefix: {}".format(options.logprefix))
    print("Logs directory: {}".format(options.logdir))
    print("Socket timeout: {}".format(options.timeout))
    print("---------")

    msg_queue = Queue()
    if options.kafka:
        import kafka_publish
        t_kafka = Thread(target=kafka_publish.publish_loop, args=(msg_queue,))
        t_kafka.start()
    
    t_from_phone = Thread(target=recv_stream_from_phone, args=(options, msg_queue,))
    t_from_phone.start()

if __name__ == "__main__":
    main()
