'''Root package of the easytrack project.'''
# pylint: disable=duplicate-code

from .utils import notnull
from . import models as mdl
from . import settings


def init(db_host: str, db_port: int, db_name: str, db_user: str, db_password: str):
    """
    Initialize the database connection.
    :param db_host: The host of the database.
    :param db_port: The port of the database.
    :param db_name: The name of the database.
    :param db_user: The user of the database.
    :param db_password: The password of the database.
    """

    # load dataset settings
    settings.POSRGRES_HOST = notnull(db_host)
    settings.POSTGRES_PORT = notnull(db_port)
    settings.POSTGRES_DBNAME = notnull(db_name)
    settings.POSTGRES_USER = notnull(db_user)
    settings.POSTGRES_PASSWORD = notnull(db_password)

    # initialize database connection
    mdl.init(
        host = settings.POSRGRES_HOST,
        port = settings.POSTGRES_PORT,
        dbname = settings.POSTGRES_DBNAME,
        user = settings.POSTGRES_USER,
        password = settings.POSTGRES_PASSWORD,
    )
