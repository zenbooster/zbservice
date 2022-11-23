#!/usr/bin/env python3
import os
import paho.mqtt.client as mqtt
import certifi

def on_connect(client, userdata, flags, rc):
    print("Connected with result code "+str(rc))

    client.subscribe("devices/zenbooster/#")

def on_message(client, userdata, msg):
    print(msg.topic+" "+str(msg.payload))

def on_log(client, userdata, level, buf):
  print("log: ",buf)

client = mqtt.Client()
client.on_connect = on_connect
client.on_message = on_message
client.on_log = on_log

client.tls_set(certifi.where())
client.username_pw_set(os.environ.get('ZB_MQTT_USER'), os.environ.get('ZB_MQTT_PASS'))
client.connect(os.environ.get('ZB_HOST'), int(os.environ.get('ZB_MQTT_PORT')), 60)

client.loop_forever()
