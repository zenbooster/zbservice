import os

from sqlalchemy import create_engine
from sqlalchemy_utils import database_exists, create_database

from sqlalchemy import Column, ForeignKey, BigInteger, SmallInteger, Float, String, Text, TIMESTAMP
from sqlalchemy import update, delete

from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base

from sqlalchemy.orm import Session

from datetime import datetime

Base = declarative_base()

class TTblDevice(Base):

    __tablename__ = 'device'
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

    session = relationship("TTblSession", back_populates="device")
    config = relationship("TTblConfig", back_populates="device")

    def __repr__(self):
        return f'{self.id} {self.mac} {self.name} {self.description}'

class TTblSession(Base):

    __tablename__ = 'session'
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
        ForeignKey('device.id'),
        comment='ИД устройства',
        nullable=False
    )
    begin = Column(TIMESTAMP, comment='начало')
    end = Column(TIMESTAMP, comment='конец')
    description = Column(Text, comment='описание сессии')

    eeg_power = relationship("TTblEegPower", back_populates="session")
    device = relationship('TTblDevice', back_populates='session')

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
        ForeignKey('session.id'),
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
    f = Column(Float, comment='значение формулы', nullable=False)

    session = relationship('TTblSession', back_populates='eeg_power')

    def __repr__(self):
        return f'{self.id} {self.id_session} {self.when} {self.poor} {self.d} {self.t} {self.al} {self.ah} {self.bl} {self.bh} {self.gl} {self.gm} {self.ea} {self.em}'

class TTblCfgNamespace(Base):

    __tablename__ = 'cfg_namespace'
    __tableargs__ = {
        'comment': 'конфигурационные пространства имён'
    }

    id = Column(
        BigInteger,
        nullable=False,
        unique=True,
        primary_key=True,
        autoincrement=True
    )
    name = Column(String(16), comment='пространство имён', nullable=False, unique=True)

    config = relationship("TTblConfig", back_populates="cfg_namespace")

    def __repr__(self):
        return f'{self.id} {self.name}'

class TTblConfig(Base):

    __tablename__ = 'config'
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
        ForeignKey('device.id'),
        comment='ИД устройства',
        nullable=False
    )
    id_cfg_namespace = Column(
        BigInteger,
        ForeignKey('cfg_namespace.id'),
        comment='ИД пространства имён',
        nullable=False
    )
    when = Column(TIMESTAMP, comment='когда', nullable=False)
    name = Column(String(16), comment='имя настройки', nullable=False)
    val = Column(Text, comment='значение')

    device = relationship('TTblDevice', back_populates='config')
    cfg_namespace = relationship('TTblCfgNamespace', back_populates='config')

    def __repr__(self):
        return f'{self.id} {self.id_device} {self.when} {self.name} {self.val}'

def get_cfg_namespace(engine, name):
    with Session(engine) as session:
        res = session.query(TTblCfgNamespace.id, TTblCfgNamespace.name).filter_by(name=name).first()
        return res

def chk_cfg_namespace(engine, name):
    cfg_namespace = None
    with Session(engine) as session:
        cfg_namespace = get_cfg_namespace(engine, name)

    if cfg_namespace is None:
        with Session(engine) as session:
            o = TTblCfgNamespace(name=name)
            session.add(o)
            session.commit()

def init_db():
    db_name = 'zenbooster'
    engine = create_engine('mysql+pymysql://{}:{}@{}:{}/{}'.
        format(
            os.environ.get('ZB_MYSQL_USER'),
            os.environ.get('ZB_MYSQL_PASS'),
            os.environ.get('ZB_MYSQL_HOST'),
            int(os.environ.get('ZB_MYSQL_PORT')),
            db_name
        ),
        pool_recycle=3600
    )
    if database_exists(engine.url):
        print('Найдена БД "{}"!'.format(db_name))
    else:
        print('БД "{}" не найдена. Создаём...'.format(db_name), end='')
        create_database(engine.url)
        print('Ok!')

    print('Проверка метаданных БД...', end='')
    Base.metadata.create_all(engine)
    chk_cfg_namespace(engine, 'option')
    chk_cfg_namespace(engine, 'formula')
    print('Ok!')

    return engine

def get_device(mac):
    with Session(engine) as session:
        res = session.query(TTblDevice.id, TTblDevice.mac).filter_by(mac=mac).first()
        return res

def get_last_opened_session(mac):
    id_device = get_device(mac).id
    with Session(engine) as session:
        sess = session.query(TTblSession.id, TTblSession.id_device, TTblSession.begin, TTblSession.end).filter_by(id_device=id_device, end=None).order_by(TTblSession.begin.desc()).first()
        return sess

def get_last_config(id_device, id_cfg_namespace, k):
    with Session(engine) as session:
        res = session.query(TTblConfig.id, TTblConfig.id_device, TTblConfig.id_cfg_namespace, TTblConfig.when, TTblConfig.name, TTblConfig.val) \
          .filter_by(id_device=id_device, id_cfg_namespace=id_cfg_namespace, name=k) \
          .order_by(TTblConfig.when.desc()) \
          .first()
        return res

def update_config(id_device, namespace, k, v):
    id_cfg_namespace = get_cfg_namespace(engine, namespace).id
    o = get_last_config(id_device, id_cfg_namespace, k)
    if (o is None) or (o.val != v):
        with Session(engine) as session:
            o = TTblConfig(id_device=id_device, id_cfg_namespace=id_cfg_namespace, when=datetime.utcnow(), name=k, val=v)
            session.add(o)
            session.commit()

def update_config_table(id_device, namespace, js):
    for k, v in js.items():
        update_config(id_device, namespace, k, v)
