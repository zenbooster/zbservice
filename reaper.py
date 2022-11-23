#!/usr/bin/env python3
import os

from sqlalchemy import create_engine
from sqlalchemy_utils import database_exists, create_database

import paho.mqtt.client as mqtt
import certifi

def init_db():
    db_name = 'zenbooster'
    engine = create_engine('mysql+pymysql://{}:{}@{}:{}/{}'.
        format(
            os.environ.get('ZB_MYSQL_USER'),
            os.environ.get('ZB_MYSQL_PASS'),
            os.environ.get('ZB_MYSQL_HOST'),
            int(os.environ.get('ZB_MYSQL_PORT')),
            db_name
        )
    )
    if database_exists(engine.url):
        print('Найдена БД "{}"!'.format(db_name))
    else:
        print('БД "{}" не найдена. Создаём...'.format(db_name), end='')
        create_database(engine.url)
        print('Ok!')

def on_connect(client, userdata, flags, rc):
    print('Connected with result code '+str(rc))

    client.subscribe('devices/zenbooster/#')

def on_message(client, userdata, msg):
    print(msg.topic+' '+str(msg.payload))

def on_log(client, userdata, level, buf):
  print('log: ', buf)

init_db()

print('Подключаемся к MQTT брокеру:')
client = mqtt.Client()
client.on_connect = on_connect
client.on_message = on_message
client.on_log = on_log

client.tls_set(certifi.where())
client.username_pw_set(os.environ.get('ZB_MQTT_USER'), os.environ.get('ZB_MQTT_PASS'))
client.connect(os.environ.get('ZB_MQTT_HOST'), int(os.environ.get('ZB_MQTT_PORT')), 60)

client.loop_forever()
