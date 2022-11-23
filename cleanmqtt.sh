#!/bin/bash
echo "cleaning " $ZB_MQTT_HOST
mosquitto_sub -h $ZB_MQTT_HOST -p $ZB_MQTT_PORT -u $ZB_MQTT_USER -P $ZB_MQTT_PASS -t "#" -v --retained-only | while read line; do echo "${line% *}"; mosquitto_pub -h $ZB_MQTT_HOST -p $ZB_MQTT_PORT -u $ZB_MQTT_USER -P $ZB_MQTT_PASS -t "${line% *}" -r -n; done
