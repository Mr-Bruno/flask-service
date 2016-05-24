import os
import contextlib
import traceback
from sqlalchemy.orm import sessionmaker, scoped_session
from sqlalchemy import create_engine, event
from sqlalchemy.ext.declarative import DeclarativeMeta
from sqlalchemy import Column, Integer, String, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.pool import NullPool

# Creation for different type of DB
if os.environ.get('MYSQL_DB') == '1':

    db_path = r'mysql://root@127.0.0.1/test'
    engine = create_engine(db_path, isolation_level='READ COMMITTED', poolclass=NullPool)

else:
    cwd = os.getcwd()
    path = os.path.join(cwd, 'test.db')
    db_path = r'sqlite:///%s' % path

    print db_path

    engine = create_engine(db_path, isolation_level='SERIALIZABLE')

Session = scoped_session(sessionmaker(bind=engine, autocommit=True))

MyBase = declarative_base()

@contextlib.contextmanager
def transaction():
    try:

        Session.begin()
        yield Session

        try:
            Session.commit()
        except:
            print 'FAILED COMMIT:\n%s', traceback.format_stack()

    except:
        print("Not expected exception caught")
        Session.rollback()
        print(traceback.format_exc())


class BaseMeta(DeclarativeMeta):
    def __new__(cls, clsname, bases, dct):
        dct['__tablename__'] = clsname.lower()
        return DeclarativeMeta.__new__(cls, clsname, bases, dct)


class Base(MyBase):
    __abstract__ = True
    __metaclass__ = BaseMeta


class Referer(Base):
    id = Column(Integer, primary_key=True)
    url = Column(String(512), index=True)

    def __init__(self, url):
        self.url = url


class Domain(Base):
    id = Column(Integer, primary_key=True)
    url = Column(String(512), index=True)

    def __init__(self, url):
        self.url = url


class Information(Base):
    id = Column(Integer, primary_key=True)
    domain_url = Column(String(512), ForeignKey('domain.url'), index=True)
    referer_url = Column(String(512), ForeignKey('referer.url'), index=True)
    creative_size = Column(String, index=True)

    def __init__(self, domain_url, referer_url, creative_size):
        self.domain_url = domain_url
        self.referer_url = referer_url
        self.creative_size = creative_size

# Generate structure
Base.metadata.create_all(engine)