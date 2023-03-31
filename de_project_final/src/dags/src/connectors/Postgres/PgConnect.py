from contextlib import contextmanager
from typing import Generator
from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError

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