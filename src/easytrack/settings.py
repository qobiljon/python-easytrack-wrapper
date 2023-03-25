'''Settings / parameters for the easytrack project.'''

# stdlib
from typing import Dict, Type
from datetime import datetime
from functools import cache

POSRGRES_HOST: str = None
POSTGRES_PORT: int = None
POSTGRES_DBNAME: str = None
POSTGRES_USER: str = None
POSTGRES_PASSWORD: str = None


class ColumnTypes:
    """
    Helper class that holds the mapping from string to python type for the
    columns in the database. This is used when exporting or importing data
    from/to the database. The mapping is as follows:
      - `"timestamp"` to python's `datetime`
      - `"text"` to python's `str`
      - `"integer"` to python's `int`
      - `"float"` to python's `float`
    """

    # pylint: disable=too-few-public-methods

    class ColumnType:
        ''' A switch between string, python, and postgres types for a column. '''

        def __init__(self, name: str, py_type: Type, pg_type: str):
            '''            
            :param str_type: string type (used in json)
            :param py_type: python type (used in python)
            :param pg_type: postgres type (used in sql queries)
            '''
            self.name = name
            self.py_type = py_type
            self.pg_type = pg_type

        def verify_value(self, value):
            ''' Verifies that the given value is of the correct type. '''
            if not isinstance(value, self.py_type):
                raise ValueError(f'Expected {self.py_type}, got {type(value)}')

    TIMESTAMP = ColumnType(
        name = 'timestamp',
        py_type = datetime,
        pg_type = 'timestamp',
    )
    TEXT = ColumnType(
        name = 'text',
        py_type = str,
        pg_type = 'text',
    )
    INTEGER = ColumnType(
        name = 'integer',
        py_type = int,
        pg_type = 'integer',
    )
    FLOAT = ColumnType(
        name = 'float',
        py_type = float,
        pg_type = 'float8',
    )

    @staticmethod
    def all():
        ''' Returns a list of all the column types. '''
        return [
            ColumnTypes.TIMESTAMP,
            ColumnTypes.TEXT,
            ColumnTypes.INTEGER,
            ColumnTypes.FLOAT,
        ]

    @cache
    @staticmethod
    def to_map() -> Dict[str, ColumnType]:
        ''' Returns a dictionary mapping string types to python types. '''
        return {column_type.name: column_type for column_type in ColumnTypes.all()}

    @staticmethod
    def from_str(str_type: str) -> ColumnType:
        ''' Returns the mapping for the given string type. '''

        # verify that the given string type is valid
        colmap = ColumnTypes.to_map()
        if str_type not in colmap:
            raise ValueError(f'Invalid column type: {str_type}')

        # return the mapping
        return colmap[str_type]
