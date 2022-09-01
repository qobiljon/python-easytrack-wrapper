from abc import ABC, abstractmethod

from psycopg2.extras import DictCursor, DictRow, _connection as PostgresConnection  # noqa
from typing import List, Dict, Iterable, Any
from datetime import timedelta as td
from datetime import datetime as dt
from random import uniform
import psycopg2 as pg2
from os import getenv
import dotenv
import pytz
import json


class Data(ABC):
	# constructor
	def __init__(self, ts: dt):
		self.ts = ts

	# private / encapsulation
	def __validate_attributes(self):
		for attribute in self.attributes:
			if not hasattr(self, attribute):
				raise AttributeError()

	# properties
	@property
	def timestamp(self) -> dt:
		return self.ts

	@property
	def value_json(self) -> object:
		res: Dict[str, Any] = dict()
		for key, value in zip(self.attributes, self.values):
			res[key] = value
		return res

	@property
	def value_str(self):
		return json.dumps(self.value_json)

	@property
	def values(self) -> List[Any]:
		self.__validate_attributes()
		for key in self.attributes:
			yield getattr(self, key)

	# abstract / polymorphism
	@property
	@abstractmethod
	def data_source_id(self) -> int:
		return NotImplemented

	@property
	@abstractmethod
	def attributes(self) -> List[str]:
		return NotImplemented


class DBHelper:
	def __init__(self, table_name: str):
		self.table_name = table_name

	def __enter__(self):
		self.con: PostgresConnection = pg2.connect(
			host=getenv('DATABASE_HOST'),
			port=getenv('DATABASE_PORT'),
			dbname=getenv('DATABASE_NAME'),
			user=getenv('DATABASE_USER'),
			password=getenv('DATABASE_PASSWORD'),
		)
		self.cur: DictCursor = self.con.cursor(cursor_factory=DictCursor)
		return self

	def __exit__(self, exc_type, exc_val, exc_tb):
		self.cur.close()
		self.con.commit()
		self.con.close()

	def populate(self, data: Iterable[Data]):
		for row in data:
			self.cur.execute(
				query=f'insert into data.{self.table_name}(data_source_id, ts, val) values(%s, %s, %s)',  # noqa
				vars=(row.data_source_id, row.timestamp, row.value_str,)
			)

	def head(self):
		self.cur.execute(f'select * from data.{self.table_name} order by ts desc limit 10')  # noqa
		rows: List[DictRow] = self.cur.fetchall()
		for row in rows:
			data_source_id = row.get('data_source_id')
			ts = row.get('ts')
			val = row.get('val')

			print(data_source_id, ts, val)


class Acceleration(Data):
	def __init__(
		self,
		ts: dt,
		x: float,
		y: float,
		z: float
	):
		super(Acceleration, self).__init__(ts=ts)
		self.x = x
		self.y = y
		self.z = z

	@property
	def attributes(self) -> List[str]:
		return ['x', 'y', 'z']

	@property
	def data_source_id(self) -> int:
		return 1

	@property
	def data(self) -> Dict[str, Any]:
		res: Dict[str, Any] = dict()
		for key in self.attributes:
			res[key] = self.__getattribute__(name=key)
		return res

	@staticmethod
	def random(ts: dt) -> 'Acceleration':
		return Acceleration(
			ts=ts,
			x=uniform(0, 8),
			y=uniform(0, 8),
			z=uniform(0, 8)
		)

	@staticmethod
	def generate(
		hz: int,
		from_ts: dt,
		till_ts: dt
	) -> Iterable['Acceleration']:
		assert hz > 0

		delta_ts = td(microseconds=int((1 / hz) * 1_000_000))
		ts = from_ts

		while ts < till_ts:
			yield Acceleration.random(ts=ts)
			ts += delta_ts


def main():
	now = dt.now(tz=pytz.timezone('Asia/Seoul'))

	with DBHelper(table_name='campaign2_user3') as db:
		db.head()
		db.populate(data=Acceleration.generate(
			hz=20,
			from_ts=now - td(days=5),
			till_ts=now
		))
		db.head()


if __name__ == '__main__':
	dotenv.load_dotenv()
	main()
