from contextlib import contextmanager
from typing import Generator, Dict

import vertica_python
from vertica_python import Connection


class VerticaConnect:
    def __init__(self, host: str, port: int, db_name: str, user: str, password: str, autocommit = True) -> None:
        self.host = host
        self.port = port
        self.db_name = db_name
        self.user = user
        self.pw = password
        self.autocommit = autocommit

    @property
    def connection_params(self) -> Dict:
        return {'host': self.host, 
                 'port': self.port, 
                 'user': self.user,  
                 'password': self.pw,         
                 'database': self.db_name,
                 'autocommit': self.autocommit}


    @contextmanager
    def connection(self) -> Generator[Connection, None, None]:
        conn = vertica_python.connect(**self.connection_params)
        try:
            yield conn
            conn.commit()
        except Exception as e:
            conn.rollback()
            raise e
        finally:
            conn.close()