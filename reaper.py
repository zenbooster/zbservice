#!/usr/bin/env python3
import traceback
import paho.mqtt.client as mqtt
import certifi
import json

from db import *

def on_connect(client, userdata, flags, rc):
    print('Connected with result code '+str(rc))

    client.subscribe('devices/zenbooster/#')

def on_message(client, userdata, msg):
    st = msg.topic
    print('topic: '+st)
    st = st[len('devices/zenbooster/'):]
    mac=st[:6*2+5]
    st = st[len(mac)+1:]
    mac = mac.translate(str.maketrans('', '', ':- '))
    print('subtopic: '+st)
    print('mac: '+mac)

    js = msg.payload.decode()
    print('data: '+js)
    js = json.loads(js)
    when = js['when']
    dt_when = datetime.utcfromtimestamp(when)

    print('when: {}'.format(dt_when))

    engine = userdata

    print(f'[begin on_{st}]')
    if st == 'hello':
        with Session(engine) as session:
            device = get_device(mac)
            is_exists = device is not None

        id_device = None
        if is_exists:
            print('Устройство "{}" уже есть в БД.'.format(mac))
            id_device = device.id
        else:
            print('Добавляем новое устройство "{}".'.format(mac))
            with Session(engine) as session:
                device = TTblDevice(mac=mac, name="zenbooster")
                session.add(device)
                session.commit()
                id_device = device.id

        update_config_table(id_device, 'option', js['options'])
        update_config_table(id_device, 'formula', js['formulas'])

        print('ИД устройства: {}'.format(id_device))

    elif st == 'session_begin':
        print('Открываем новую сессию для устройства "{}".'.format(mac))
        with Session(engine) as session:
            sess = TTblSession(id_device=get_device(mac).id, begin=dt_when)
            session.add(sess)
            session.commit()
            id_session = sess.id
        print('ИД сессии: {}'.format(id_session))

    elif st == 'eeg_power':
        id_session = get_last_opened_session(mac).id
        print('Добавляем данные в сессию с ИД {}.'.format(id_session))

        with Session(engine) as session:
            eeg_power = TTblEegPower(
                id_session=get_last_opened_session(mac).id,
                when=dt_when,
                poor=js['poor'],
                d=js['d'],
                t=js['t'],
                al=js['al'],
                ah=js['ah'],
                bl=js['bl'],
                bh=js['bh'],
                gl=js['gl'],
                gm=js['gm'],
                ea=js['ea'],
                em=js['em'],
                f=js['f']
            )
            session.add(eeg_power)
            session.commit()

    elif st == 'session_end':
        print('Закрываем сессию для устройства "{}".'.format(mac))
        id_session = get_last_opened_session(mac).id
        print('ИД закрываемой сессии: {}'.format(id_session))

        with Session(engine) as session:
            with session.begin():
                session.execute(
                    update(TTblSession)
                    .where(TTblSession.id==id_session)
                    .values(end=dt_when)
                )
            session.commit()

    elif st == 'session_cancel':
        print('Удаляем сессию и связанные данные для устройства "{}".'.format(mac))
        id_session = get_last_opened_session(mac).id
        print('ИД удаляемой сессии: {}'.format(id_session))

        with Session(engine) as session:
            session.execute(delete(TTblEegPower).where(TTblEegPower.id_session==id_session))
            session.execute(delete(TTblSession).where(TTblSession.id==id_session))
            session.commit()

    elif st == 'bye':
        pass

    print(f'[end on_{st}]\n')

def on_log(client, userdata, level, buf):
  print('log: ', buf)

if __name__ == "__main__":
    while(True):
        try:
            engine = init_db()
            print('Подключаемся к MQTT брокеру:')
            client = mqtt.Client(userdata=engine)
            client.on_connect = on_connect
            client.on_message = on_message
            client.on_log = on_log

            client.tls_set(certifi.where())
            client.username_pw_set(os.environ.get('ZB_MQTT_USER'), os.environ.get('ZB_MQTT_PASS'))
            client.connect(os.environ.get('ZB_MQTT_HOST'), int(os.environ.get('ZB_MQTT_PORT')), 60)

            client.loop_forever()
        except KeyboardInterrupt:
            exit(0)
        except:
            print('Произошло исключение, восстанавливаемся...')
            traceback.print_exc()
