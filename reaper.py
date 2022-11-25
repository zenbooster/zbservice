#!/usr/bin/env python3
import os

from sqlalchemy import create_engine
from sqlalchemy_utils import database_exists, create_database

from sqlalchemy import Column, ForeignKey, BigInteger, SmallInteger, Float, String, Text, TIMESTAMP
from sqlalchemy import update

from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base

from sqlalchemy.orm import Session

import paho.mqtt.client as mqtt
import certifi

import json
from datetime import datetime

Base = declarative_base()

class TTblDevices(Base):

    __tablename__ = 'devices'
    __tableargs__ = {
        'comment': 'устройства'
    }

    id = Column(
        BigInteger,
        nullable=False,
        unique=True,
        primary_key=True,
        autoincrement=True
    )
    mac = Column(
        String(12),
        comment='MAC устройства',
        nullable=False,
        unique=True,
    )
    name = Column(
        Text,
        comment='наименование устройства',
        nullable=False
    )
    description = Column(Text, comment='описание устройства')

    sessions = relationship("TTblSessions", back_populates="devices")
    options = relationship("TTblOptions", back_populates="devices")

    def __repr__(self):
        return f'{self.id} {self.mac} {self.name} {self.description}'

class TTblSessions(Base):

    __tablename__ = 'sessions'
    __tableargs__ = {
        'comment': 'сеансы'
    }

    id = Column(
        BigInteger,
        nullable=False,
        unique=True,
        primary_key=True,
        autoincrement=True
    )
    id_device = Column(
        BigInteger,
        ForeignKey('devices.id'),
        comment='ИД устройства',
        nullable=False
    )
    begin = Column(TIMESTAMP, comment='начало')
    end = Column(TIMESTAMP, comment='конец')
    description = Column(Text, comment='описание сессии')

    eeg_power = relationship("TTblEegPower", back_populates="sessions")
    devices = relationship('TTblDevices', back_populates='sessions')

    def __repr__(self):
        return f'{self.id} {self.id_device} {self.begin} {self.end} {self.description}'

class TTblEegPower(Base):

    __tablename__ = 'eeg_power'
    __tableargs__ = {
        'comment': 'ритмы'
    }

    id = Column(
        BigInteger,
        nullable=False,
        unique=True,
        primary_key=True,
        autoincrement=True
    )
    id_session = Column(
        BigInteger,
        ForeignKey('sessions.id'),
        comment='ИД сеанса',
        nullable=False
    )
    when = Column(TIMESTAMP, comment='когда', nullable=False)

    poor = Column(SmallInteger, comment='качество сигнала', nullable=False)

    d = Column(Float, comment='дельта', nullable=False)
    t = Column(Float, comment='тета', nullable=False)
    al = Column(Float, comment='нижняя альфа', nullable=False)
    ah = Column(Float, comment='верхняя альфа', nullable=False)
    bl = Column(Float, comment='нижняя бета', nullable=False)
    bh = Column(Float, comment='верхняя бета', nullable=False)
    gl = Column(Float, comment='нижняя гамма', nullable=False)
    gm = Column(Float, comment='средняя гамма', nullable=False)
    ea = Column(Float, comment='esense внимание', nullable=False)
    em = Column(Float, comment='esense медитация', nullable=False)

    sessions = relationship('TTblSessions', back_populates='eeg_power')

    def __repr__(self):
        return f'{self.id} {self.id_device} {self.id_session} {self.when} {self.poor} {self.d} {self.t} {self.al} {self.ah} {self.bl} {self.bh} {self.gl} {self.gm} {self.ea} {self.em}'

class TTblOptions(Base):

    __tablename__ = 'options'
    __tableargs__ = {
        'comment': 'опции'
    }

    id = Column(
        BigInteger,
        nullable=False,
        unique=True,
        primary_key=True,
        autoincrement=True
    )
    id_device = Column(
        BigInteger,
        ForeignKey('devices.id'),
        comment='ИД устройства',
        nullable=False
    )
    when = Column(TIMESTAMP, comment='когда')
    name = Column(String(16), comment='имя опции')
    val = Column(Text, comment='значение')

    devices = relationship('TTblDevices', back_populates='options')

    def __repr__(self):
        return f'{self.id} {self.id_device} {self.when} {self.name} {self.val}'

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

    print('Проверка метаданных БД...', end='')
    Base.metadata.create_all(engine)
    print('Ok!')

    return engine

def on_connect(client, userdata, flags, rc):
    print('Connected with result code '+str(rc))

    client.subscribe('devices/zenbooster/#')

def get_device(mac):
    with Session(engine) as session:
        device = session.query(TTblDevices.id, TTblDevices.mac).filter_by(mac=mac).first()
        return device

def get_last_opened_session(mac):
    id_device = get_device(mac).id
    with Session(engine) as session:
        sess = session.query(TTblSessions.id, TTblSessions.id_device, TTblSessions.begin, TTblSessions.end).filter_by(id_device=id_device, end=None).order_by(TTblSessions.begin.desc()).first()
        return sess

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

    if st == 'hello':
        print(f'begin on_{st}')
        with Session(engine) as session:
            #device = session.query(TTblDevices.id, TTblDevices.mac).filter_by(mac=mac).first()
            device = get_device(mac)
            is_exists = device is not None

        id_device = None
        if is_exists:
            print('Устройство "{}" уже есть в БД.'.format(mac))
            id_device = device.id
        else:
            print('Добавляем новое устройство "{}".'.format(mac))
            with Session(engine) as session:
                device = TTblDevices(mac=mac, name="zenbooster")
                session.add(device)
                session.commit()
                id_device = device.id

        print('ИД устройства: {}'.format(id_device))
        print(f'end on_{st}')

    elif st == 'session_begin':
        print(f'begin on_{st}')
        print('Открываем новую сессию для устройства "{}".'.format(mac))
        with Session(engine) as session:
            sess = TTblSessions(id_device=get_device(mac).id, begin=dt_when)
            session.add(sess)
            session.commit()
            id_session = sess.id
        print('ИД сессии: {}'.format(id_session))
        print(f'end on_{st}')

    elif st == 'eeg_power':
        print(f'begin on_{st}')
        id_session = get_last_opened_session(mac).id
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
                em=js['em']
            )
            session.add(eeg_power)
            session.commit()
        print(f'end on_{st}')

    elif st == 'session_end':
        print(f'begin on_{st}')
        print('Закрываем сессию для устройства "{}".'.format(mac))
        id_session = get_last_opened_session(mac).id
        print('ИД закрываемой сессии: {}'.format(id_session))

        with Session(engine) as session:
            with session.begin():
                session.execute(
                    update(TTblSessions)
                    .where(TTblSessions.id==id_session)
                    .values(end=dt_when)
                )
        session.commit()
        print(f'end on_{st}')

    elif st == 'bye':
        print(f'begin on_{st}')
        print(f'end on_{st}')

def on_log(client, userdata, level, buf):
  print('log: ', buf)

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
