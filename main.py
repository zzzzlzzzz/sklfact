from argparse import ArgumentParser, Namespace
from datetime import datetime

import requests
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine, Column, String, DateTime
from sqlalchemy.orm import sessionmaker


API_ENDPOINT = 'http://analytics.skillfactory.ru:5000/api/v1.0.1/get_structure_course/'

Base = declarative_base()


class Module(Base):
    __tablename__ = 'module'
    module_id = Column(String(128), primary_key=True)
    module_name = Column(String(128), nullable=False)
    last_update = Column(DateTime, nullable=False, default=datetime.utcnow())

    def __init__(self, module_id: str, module_name: str):
        self.module_id = module_id
        self.module_name = module_name


def cli() -> 'Namespace':
    parser = ArgumentParser()
    parser.add_argument('connection', help='DB connection string (mysql://user:password@hostname/dbname for mysql)', metavar='CONNECTION')
    parser.add_argument('-t', '--timeout', default=60, type=int, help='API timeout', metavar='TIMEOUT')
    return parser.parse_args()


def main(args: 'Namespace') -> None:
    response = requests.post(API_ENDPOINT, timeout=args.timeout)
    response.raise_for_status()
    response = response.json()

    engine = create_engine(args.connection, echo=True)
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    session = sessionmaker(engine)()
    try:
        for block in response['blocks'].values():
            block_id = block['id']
            block_name = block['display_name']
            print('{:<128}{}'.format(block_name, block_id))
            session.add(Module(block_id, block_name))
        session.commit()
    except Exception as e:
        session.rollback()
        raise e
    finally:
        session.close()


if __name__ == '__main__':
    main(cli())
