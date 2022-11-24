#!/usr/bin/env python3
import os

from sqlalchemy import create_engine
from sqlalchemy_utils import database_exists, create_database

from sqlalchemy import Column, ForeignKey, BigInteger, SmallInteger, Float, String, Text, TIMESTAMP
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base

import paho.mqtt.client as mqtt
import certifi

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

    device = relationship('TTblDevices', backref='sessions_device', lazy='subquery')

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
    id_device = Column(
        BigInteger,
        ForeignKey('devices.id'),
        comment='ИД устройства',
        nullable=False
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

    device = relationship('TTblDevices', backref='eeg_power_device', lazy='subquery')
    session = relationship('TTblDevices', backref='eeg_power_session', lazy='subquery')

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

    device = relationship('TTblDevices', backref='options_device', lazy='subquery')

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
