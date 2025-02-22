#!/usr/bin/env python3

import socket
import os
import argparse

HOST = "127.0.0.1"  # The server's hostname or IP address
COMMAND_PORT_ANDROID = 12345  # The port at the which the Android App listens for commands
DATA_PORT = 12348 # The port on this compute node, to which the Android App sends measurements

PORT_FORWARD_COMMAND = "adb forward tcp:" + str(COMMAND_PORT_ANDROID) + " tcp:" + str(COMMAND_PORT_ANDROID)
PORT_REVERSE_COMMAND = "adb reverse tcp:" + str(DATA_PORT) + " tcp:" + str(DATA_PORT)

def sendToAndroid(command):
    # While setting up the tcp forwarding through adb should be a one time activity, currently I don't have a way 
    # to check if the setup was interrupted in between, e.g. by a loose cable. Assuming the worst case, always repeat the setup.
    os.system(PORT_FORWARD_COMMAND)
    if "start" in command:
        os.system(PORT_REVERSE_COMMAND)
    
    # Send the command to the Android
    command = command + "\n"
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.connect((HOST, COMMAND_PORT_ANDROID))
        print("Connected to Android. Sending command: " + str(command))
        s.send(bytes(command, "utf-8"))
        data = s.recv(1024)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('command', type=str, help='command to send to the Android device')
    args = parser.parse_args()

    if args.command is not None:
        sendToAndroid(args.command)
