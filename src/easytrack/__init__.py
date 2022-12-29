import psycopg2 as pg2
import psycopg2.extras as pg2_extras

from .utils import notnull
from . import models as mdl

postgres_host: str = None
postgres_port: int = None
postgres_dbname: str = None
postgres_user: str = None
postgres_password: str = None


def init(db_host: str, db_port: int, db_name: str, db_user: str, db_password: str):
  global postgres_host, postgres_port, postgres_dbname, postgres_user, postgres_password

  postgres_host = notnull(db_host)
  postgres_port = notnull(db_port)
  postgres_dbname = notnull(db_name)
  postgres_user = notnull(db_user)
  postgres_password = notnull(db_password)

  mdl.init(
    host = postgres_host,
    port = postgres_port,
    dbname = postgres_dbname,
    user = postgres_user,
    password = postgres_password,
  )
