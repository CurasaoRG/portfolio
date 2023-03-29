import vertica_python

class VerticaConnector:
    """
    Класс для подключения к Vertica.
    """
    def __init__(self,
                 host : str,
                 port: int,
                 user: str,
                 password: str,
                 ssl=False,
                 autocommit=True                 
                 ) -> None:
        self._host = host
        self._port = port
        self._user = user
        self._password = password
        self._ssl = ssl
        self._autocommit = autocommit
  
    def connection(self):
        return vertica_python.connect(
            host = self._host,
            port=self._port,
            user=self._user,
            password = self._password,
            ssl = self._ssl,
            autocommit = self._autocommit
        )