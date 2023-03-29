from contextlib import contextmanager
from typing import Generator
from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError

# engine = create_engine("postgresql+psycopg2://student:de_student_112022@rc1b-w5d285tmxa8jimyn.mdb.yandexcloud.net:6432/db1")
# dbConnection = engine.connect()


class PgConnect:
    def __init__(self, host: str, port: int, db_name: str, user: str, password: str) -> None:
        self.host = host
        self.port = port
        self.db_name = db_name
        self.user = user
        self.password = password

    def url(self) -> str:
        return f"postgresql+psycopg2://{self.user}:{self.password}@{self.host}:{self.port}/{self.db_name}"
    
    def connection(self):
        self._engine = create_engine(self.url())
        return self._engine.connect()


    # @contextmanager
    # def connection(self):
    #     engine = create_engine(self.url())
    #     conn = engine.connect()
    #     try:
    #         yield conn
    #         conn.commit()
    #     except Exception as e:
    #         conn.rollback()
    #         raise e
    #     finally:
    #         conn.close()
