from psycopg2 import extras as psycopg2_extras
from boilerplate.utils import settings
from boilerplate import utils
import psycopg2
import json
import os


# region common part
def get_db_connection():
	if settings.db_conn is None:
		settings.db_conn = psycopg2.connect(
			host='127.0.0.1',
			database='easytrack_db',
			user='postgres',
			password='postgres'
		)
		print('database initialized', settings.db_conn)
	return settings.db_conn


def end():
	get_db_connection().close()


def extract_value(row, column_name, default_value=None):
	if row is None:
		return default_value
	elif row[column_name] is None:
		return default_value
	else:
		return row[column_name]


# endregion


# region 1. user management
def create_user(id_token, name, email):
	cur = get_db_connection().cursor(cursor_factory=psycopg2_extras.DictCursor)
	cur.execute('insert into "et"."user"("id_token", "name", "email") values (%s,%s,%s);', (
		id_token,
		name,
		email
	))
	cur.close()
	get_db_connection().commit()
	return get_user(email=email)


def get_user(email=None, user_id=None):
	cur = get_db_connection().cursor(cursor_factory=psycopg2_extras.DictCursor)
	row = None
	if None not in [user_id, email]:
		cur.execute('select * from "et"."user" where "id"=%s and "email"=%s;', (
			user_id,
			email,
		))
		row = cur.fetchone()
	elif user_id is not None:
		cur.execute('select * from "et"."user" where "id"=%s;', (
			user_id,
		))
		row = cur.fetchone()
	elif email is not None:
		cur.execute('select * from "et"."user" where "email"=%s;', (
			email,
		))
		row = cur.fetchone()
	cur.close()
	return row


def bind_participant_to_campaign(db_user, db_campaign):
	cur = get_db_connection().cursor(cursor_factory=psycopg2_extras.DictCursor)
	cur.execute('select exists( select 1 from "stats"."campaign_participant_stats" where "campaign_id"=%s and "user_id"=%s);', (
		db_campaign['id'],
		db_user['id']
	))
	if cur.fetchone()[0]:
		cur.close()
		return False  # old binding
	else:
		cur.execute('insert into "stats"."campaign_participant_stats"("user_id", "campaign_id", "join_timestamp")  values (%s,%s,%s) on conflict do nothing;', (
			db_user['id'],
			db_campaign['id'],
			utils.timestamp_now_ms()
		))
		cur.execute('update "et"."user" set "campaign_id" = %s where "id"=%s;', (
			db_campaign['id'],
			db_user['id']
		))
		cur.close()
		get_db_connection().commit()
		return True  # new binding


def user_is_bound_to_campaign(db_user, db_campaign):
	cur = get_db_connection().cursor(cursor_factory=psycopg2_extras.DictCursor)
	cur.execute('select exists(select * from "stats"."campaign_participant_stats" where "user_id"=%s and "campaign_id"=%s) as "exists";', (
		db_user['id'],
		db_campaign['id']
	))
	exists = cur.fetchone()['exists']
	cur.close()
	return exists


# endregion


# region 2. campaign management
def register_new_campaign(db_user_creator, name, notes, configurations, start_timestamp, end_timestamp, remove_inactive_users_timeout):
	cur = get_db_connection().cursor(cursor_factory=psycopg2_extras.DictCursor)
	cur.execute('insert into "et"."campaign"("creator_id", "name", "notes", "config_json", "start_timestamp", "end_timestamp", "remove_inactive_users_timeout") values (%s,%s,%s,%s,%s,%s,%s);', (
		db_user_creator['id'],
		name,
		notes,
		configurations,
		start_timestamp,
		end_timestamp,
		remove_inactive_users_timeout
	))
	cur.close()
	get_db_connection().commit()


def update_campaign(db_campaign, name, notes, configurations, start_timestamp, end_timestamp, remove_inactive_users_timeout):
	cur = get_db_connection().cursor(cursor_factory=psycopg2_extras.DictCursor)
	cur.execute('update "et"."campaign" set "name" = %s, "notes" = %s, "config_json" = %s, "start_timestamp" = %s, "end_timestamp" = %s, "remove_inactive_users_timeout" = %s where "id"=%s;', (
		name,
		notes,
		configurations,
		start_timestamp,
		end_timestamp,
		remove_inactive_users_timeout,
		db_campaign['id']
	))
	cur.close()
	get_db_connection().commit()


def get_campaign(campaign_id, db_creator_user=None):
	cur = get_db_connection().cursor(cursor_factory=psycopg2_extras.DictCursor)
	if db_creator_user is None:
		cur.execute('select * from "et"."campaign" where "id"=%s;', (
			campaign_id,
		))
	else:
		cur.execute('select * from "et"."campaign" where "id"=%s and "creator_id"=%s;', (
			campaign_id,
			db_creator_user['id']
		))
	row = cur.fetchone()
	cur.close()
	get_db_connection().commit()
	return row


def delete_campaign(db_campaign):
	cur = get_db_connection().cursor(cursor_factory=psycopg2_extras.DictCursor)
	cur.execute(f'delete from "et"."campaign" where id=%s;', (
		db_campaign['id'],
	))
	cur.close()
	get_db_connection().commit()


def get_campaigns(db_creator_user=None):
	cur = get_db_connection().cursor(cursor_factory=psycopg2_extras.DictCursor)
	if db_creator_user is None:
		cur.execute('select * from "et"."campaign";')
	else:
		cur.execute('select * from "et"."campaign" where "creator_id"=%s;', (
			db_creator_user['id'],
		))
	rows = cur.fetchall()
	cur.close()
	return rows


def get_campaign_participants_count(db_campaign=None):
	cur = get_db_connection().cursor(cursor_factory=psycopg2_extras.DictCursor)
	if db_campaign is None:
		cur.execute('select count(*) as "participant_count" from "et"."user" where true;')
	else:
		cur.execute('select count(*) as "participant_count" from "et"."user" where "id" in (select "user_id" from "stats"."campaign_participant_stats" where "campaign_id"=%s);', (
			db_campaign['id'],
		))
	participant_count = cur.fetchone()['participant_count']
	cur.close()
	return participant_count


def get_campaign_participants(db_campaign=None):
	cur = get_db_connection().cursor(cursor_factory=psycopg2_extras.DictCursor)
	if db_campaign is None:
		cur.execute('select * from "et"."user" where "id_token" is not null;')
	else:
		cur.execute('select * from "et"."user" where "id" in (select "user_id" from "stats"."campaign_participant_stats" where "campaign_id"=%s);', (
			db_campaign['id'],
		))
	rows = cur.fetchall()
	cur.close()
	return rows


# endregion


# region 3. data source management
def register_data_source(db_creator_user, name, icon_name):
	cur = get_db_connection().cursor(cursor_factory=psycopg2_extras.DictCursor)
	cur.execute('insert into "et"."data_source"("creator_id", "name", "icon_name") values (%s,%s,%s) returning "id";', (
		db_creator_user['id'],
		name,
		icon_name
	))
	data_source_id = cur.fetchone()["id"]
	cur.close()
	get_db_connection().commit()
	return data_source_id


def get_data_source(data_source_name=None, data_source_id=None):
	cur = get_db_connection().cursor(cursor_factory=psycopg2_extras.DictCursor)
	row = None
	if None not in [data_source_id, data_source_name]:
		cur.execute('select * from "et"."data_source" where "id"=%s and "name"=%s;', (
			data_source_id,
			data_source_name,
		))
		row = cur.fetchone()
	elif data_source_id is not None:
		cur.execute('select * from "et"."data_source" where "id"=%s;', (
			data_source_id,
		))
		row = cur.fetchone()
	elif data_source_name is not None:
		cur.execute('select * from "et"."data_source" where "name"=%s;', (
			data_source_name,
		))
		row = cur.fetchone()
	cur.close()
	return row


def get_data_source_id(data_source_name):
	db_data_source = get_data_source(data_source_name=data_source_name)
	if db_data_source is not None:
		return db_data_source['id']
	else:
		return None


def get_all_data_sources():
	cur = get_db_connection().cursor(cursor_factory=psycopg2_extras.DictCursor)
	cur.execute('select * from "et"."data_source";')
	rows = cur.fetchall()
	cur.close()
	return rows


def get_campaign_data_sources(db_campaign):
	db_data_sources = []
	config_jsons = json.loads(s=db_campaign['config_json'])
	for config_json in config_jsons:
		db_data_source = get_data_source(data_source_id=config_json['data_source_id'])
		if db_data_source is not None:
			db_data_sources += [db_data_source]
	return db_data_sources


# endregion


# region 4. data management
def fast_store_data_record(cur, user_id, campaign_id, data_source_id, timestamp, value):
	cur.execute(f'insert into "data"."{campaign_id}-{user_id}"("timestamp", "value", "data_source_id") values (%s,%s,%s) on conflict do nothing returning true;', (
		timestamp,
		psycopg2.Binary(value),
		data_source_id,
	))
	return cur.fetchone() is not None


def store_data_record(db_user, db_campaign, db_data_source, timestamp, value):
	cur = get_db_connection().cursor(cursor_factory=psycopg2_extras.DictCursor)
	fast_store_data_record(cur=cur, user_id=db_user['id'], campaign_id=db_campaign['id'], data_source_id=db_data_source['id'], timestamp=timestamp, value=value)
	cur.close()
	get_db_connection().commit()


def store_data_records(db_user, db_campaign, timestamp_list, data_source_id_list, value_list):
	cur = get_db_connection().cursor(cursor_factory=psycopg2_extras.DictCursor)
	data_sources: dict = {}
	for timestamp, data_source_id, value in zip(timestamp_list, data_source_id_list, value_list):
		if data_source_id not in data_sources:
			tmp_db_data_source = get_data_source(data_source_id=data_source_id)
			if tmp_db_data_source is None:
				continue
			else:
				data_sources[data_source_id] = (tmp_db_data_source, 0, timestamp)
		db_data_source, amount, last_timestamp = data_sources[data_source_id]
		if db_data_source is not None:
			fast_store_data_record(
				cur=cur,
				user_id=db_user['id'],
				campaign_id=db_campaign['id'],
				data_source_id=db_data_source['id'],
				timestamp=timestamp,
				value=value
			)
	cur.close()
	get_db_connection().commit()


def get_next_k_data_records(db_user, db_campaign, from_record_id, db_data_source, k):
	cur = get_db_connection().cursor(cursor_factory=psycopg2_extras.DictCursor)
	cur.execute(f'select * from "data"."{db_campaign["id"]}-{db_user["id"]}" where "id">=%s and "data_source_id"=%s order by "id" limit({k});', (
		from_record_id,
		db_data_source['id']
	))
	k_records = cur.fetchall()
	cur.close()
	return k_records


def get_filtered_data_records(db_user, db_campaign, db_data_source, from_timestamp, till_timestamp=-1):
	cur = get_db_connection().cursor(cursor_factory=psycopg2_extras.DictCursor)
	if till_timestamp > 0:
		cur.execute(f'select * from "data"."{db_campaign["id"]}-{db_user["id"]}" where "data_source_id"=%s and "timestamp">=%s and "timestamp"<%s order by "timestamp" asc;', (
			db_data_source['id'],
			from_timestamp,
			till_timestamp
		))
	else:
		cur.execute(f'select * from "data"."{db_campaign["id"]}-{db_user["id"]}" where "data_source_id"=%s and "timestamp">=%s order by "timestamp" asc limit 500;', (
			db_data_source['id'],
			from_timestamp
		))
	data_records = cur.fetchall()
	cur.close()
	return data_records


def dump_data(db_campaign, db_user):
	cur = get_db_connection().cursor(cursor_factory=psycopg2_extras.DictCursor)

	file_path = utils.get_download_file_path(f'{db_campaign["id"]}-{db_user["id"]}.bin.tmp')
	cur.execute(f'copy (select "id", "timestamp", "value", "data_source_id" from "data"."{db_campaign["id"]}-{db_user["id"]}") to %s with binary;', (file_path,))

	cur.close()
	return file_path


def dump_csv_data(db_campaign, db_user=None, db_data_source=None):
	cur = get_db_connection().cursor(cursor_factory=psycopg2_extras.DictCursor)

	if db_user is not None:
		file_path = utils.get_download_file_path(f'campaign-{db_campaign["id"]} user-{db_user["id"]}.csv')
		tmp_file_path = utils.get_download_file_path(f'{db_campaign["id"]}-{db_user["id"]}.csv')
		cur.execute(f'copy (select "data_source_id", "timestamp", "value" from "data"."{db_campaign["id"]}-{db_user["id"]}") to %s delimiter \',\' csv header;', (tmp_file_path,))
		with open(file_path, 'a') as w, open(tmp_file_path, 'r') as r:
			rows = r.readlines()
			w.write(rows[0])
			for line in rows[1:]:
				cells = line[:-1].split(',')
				cells[-1] = str(bytes.fromhex(cells[-1][2:]), encoding='utf8')
				w.write(f'{",".join(cells)}\n')

	elif db_data_source is not None:
		file_path = utils.get_download_file_path(f'campaign-{db_campaign["id"]} data_source-{db_data_source["id"]}.csv')
		for index, db_user in enumerate(get_campaign_participants(db_campaign=db_campaign)):
			sub_file_path = utils.get_download_file_path(f'{db_campaign["id"]}-{db_user["id"]}.csv')
			cur.execute(f'copy (select "timestamp", "value" from "data"."{db_campaign["id"]}-{db_user["id"]}" where "data_source_id"={db_data_source["id"]}) to %s delimiter \',\' csv header;', (sub_file_path,))
			with open(file_path, 'a') as w, open(sub_file_path, 'r') as r:
				rows = r.readlines()
				if index == 0:
					w.write(f'user_id,{rows[0]}')
				for line in rows[1:]:
					cells = line[:-1].split(',')
					cells[-1] = str(bytes.fromhex(cells[-1][2:]), encoding='utf8')
					w.write(f'{db_user["id"]},{",".join(cells)}\n')
			os.remove(sub_file_path)
	else:
		file_path = utils.get_download_file_path(f'campaign-{db_campaign["id"]}.csv')
		for index, db_user in enumerate(get_campaign_participants(db_campaign=db_campaign)):
			sub_file_path = utils.get_download_file_path(f'{db_campaign["id"]}-{db_user["id"]}.csv')
			cur.execute(f'copy (select "data_source_id", "timestamp", "value" from "data"."{db_campaign["id"]}-{db_user["id"]}") to %s delimiter \',\' csv header;', (sub_file_path,))
			with open(file_path, 'a') as w, open(sub_file_path, 'r') as r:
				rows = r.readlines()
				if index == 0:
					w.write(f'user_id,{rows[0]}')
				for line in rows[1:]:
					cells = line[:-1].split(',')
					cells[-1] = str(bytes.fromhex(cells[-1][2:]), encoding='utf8')
					w.write(f'{db_user["id"]},{",".join(cells)}\n')
			os.remove(sub_file_path)
	cur.close()
	return file_path


# endregion


# region 5. communication management
def create_direct_message(db_source_user, db_target_user, subject, content):
	cur = get_db_connection().cursor(cursor_factory=psycopg2_extras.DictCursor)
	cur.execute('insert into "et"."direct_message"("src_user_id", "target_user_id", "timestamp", "subject", "content")  values (%s,%s,%s,%s,%s);', (
		db_source_user['id'],
		db_target_user['id'],
		utils.timestamp_now_ms(),
		subject,
		content
	))
	cur.close()
	get_db_connection().commit()


def get_unread_direct_messages(db_user):
	cur = get_db_connection().cursor(cursor_factory=psycopg2_extras.DictCursor)
	cur.execute('select * from "et"."direct_message" where "target_user_id"=%s and "read"=FALSE;', (
		db_user['id'],
	))
	rows = cur.fetchall()
	cur.execute('update "et"."direct_message" set "read"=TRUE where trg_user_id=%s;', (
		db_user['id'],
	))
	cur.close()
	get_db_connection().commit()
	return rows


def create_notification(db_target_user, db_campaign, timestamp, subject, content):
	cur = get_db_connection().cursor(cursor_factory=psycopg2_extras.DictCursor)
	cur.execute('insert into "et"."notification"("target_user_id", "campaign_id", "timestamp", "subject", "content") values (%s,%s,%s,%s,%s)', (
		db_target_user['id'],
		db_campaign['id'],
		timestamp,
		subject,
		content
	))
	cur.close()
	get_db_connection().commit()


def get_unread_notifications(db_user):
	cur = get_db_connection().cursor(cursor_factory=psycopg2_extras.DictCursor)
	cur.execute('select * from "et"."notification" where "target_user_id"=%s and "read"=FALSE;', (
		db_user['id'],
	))
	rows = cur.fetchall()
	cur.execute('update "et"."notification" set "read"=TRUE where "target_user_id"=%s;', (
		db_user['id'],
	))
	cur.close()
	get_db_connection().commit()
	return rows


# endregion


# region 6. statistics
def get_participant_join_timestamp(db_user, db_campaign):
	cur = get_db_connection().cursor(cursor_factory=psycopg2_extras.DictCursor)
	cur.execute('select "join_timestamp" as "join_timestamp" from "stats"."campaign_participant_stats" where "user_id"=%s and "campaign_id"=%s;', (
		db_user['id'],
		db_campaign['id']
	))
	join_timestamp = cur.fetchone()['join_timestamp']
	cur.close()
	return join_timestamp


def get_participant_last_sync_timestamp(db_user, db_campaign):
	cur = get_db_connection().cursor(cursor_factory=psycopg2_extras.DictCursor)
	cur.execute('select max("sync_timestamp") as "last_sync_timestamp" from "stats"."per_data_source_stats" where "campaign_id"=%s and "user_id"=%s;', (
		db_campaign['id'],
		db_user['id'],
	))
	last_sync_timestamp = extract_value(row=cur.fetchone(), column_name='last_sync_timestamp', default_value=0)
	cur.close()
	return last_sync_timestamp


def get_participant_heartbeat_timestamp(db_user, db_campaign):
	cur = get_db_connection().cursor(cursor_factory=psycopg2_extras.DictCursor)
	cur.execute('select "last_heartbeat_timestamp" as "last_heartbeat_timestamp" from "stats"."campaign_participant_stats" where "user_id" = %s and "campaign_id" = %s;', (
		db_user['id'],
		db_campaign['id']
	))
	last_heartbeat_timestamp = cur.fetchone()['last_heartbeat_timestamp']
	cur.close()
	return last_heartbeat_timestamp


def get_participants_amount_of_data(db_user, db_campaign):
	cur = get_db_connection().cursor(cursor_factory=psycopg2_extras.DictCursor)
	cur.execute(f'select sum("amount_of_samples") as "amount_of_samples" from "stats"."per_data_source_stats" where "campaign_id"=%s and "user_id"=%s;', (
		db_campaign['id'],
		db_user['id'],
	))
	amount_of_samples = extract_value(row=cur.fetchone(), column_name='amount_of_samples', default_value=0)
	cur.close()
	return 0 if amount_of_samples is None else amount_of_samples


def get_participants_per_data_source_stats(db_user, db_campaign):
	cur = get_db_connection().cursor(cursor_factory=psycopg2_extras.DictCursor)
	db_data_sources = get_campaign_data_sources(db_campaign=db_campaign)
	res_stats = []
	for db_data_source in db_data_sources:
		cur.execute(f'select "amount_of_samples" as "amount_of_samples" from "stats"."per_data_source_stats" where "campaign_id"=%s and "user_id"=%s and "data_source_id"=%s;', (
			db_campaign['id'],
			db_user['id'],
			db_data_source['id'],
		))
		amount_of_samples = extract_value(row=cur.fetchone(), column_name='amount_of_samples', default_value=0)
		cur.execute(f'select "sync_timestamp" as "sync_timestamp" from "stats"."per_data_source_stats" where "campaign_id"=%s and "user_id"=%s and "data_source_id"=%s;', (
			db_campaign['id'],
			db_user['id'],
			db_data_source['id'],
		))
		sync_timestamp = extract_value(row=cur.fetchone(), column_name='sync_timestamp', default_value=0)
		res_stats += [(
			db_data_source,
			amount_of_samples,
			sync_timestamp
		)]
	cur.close()
	return res_stats


def update_user_heartbeat_timestamp(db_user, db_campaign):
	cur = get_db_connection().cursor(cursor_factory=psycopg2_extras.DictCursor)
	cur.execute('update "stats"."campaign_participant_stats" set "last_heartbeat_timestamp" = %s where "user_id" = %s and "campaign_id" = %s;', (
		utils.timestamp_now_ms(),
		db_user['id'],
		db_campaign['id']
	))
	cur.close()
	get_db_connection().commit()


def remove_participant_from_campaign(db_user, db_campaign):
	cur = get_db_connection().cursor(cursor_factory=psycopg2_extras.DictCursor)
	cur.execute('delete from "stats"."campaign_participant_stats" where "user_id" = %s and "campaign_id" = %s;', (
		db_user['id'],
		db_campaign['id']
	))
	cur.close()
	get_db_connection().commit()


def get_participants_data_source_sync_timestamps(db_user, db_campaign, db_data_source):
	cur = get_db_connection().cursor(cursor_factory=psycopg2_extras.DictCursor)
	cur.execute(f'select "sync_timestamp" as "sync_timestamp" from "stats"."per_data_source_stats" where "campaign_id"=%s and "user_id"=%s and "data_source_id"=%s;', (
		db_campaign['id'],
		db_user['id'],
		db_data_source['id'],
	))
	sync_timestamp = extract_value(row=cur.fetchone(), column_name='sync_timestamp', default_value=0)
	cur.close()
	get_db_connection().commit()
	return 0 if sync_timestamp is None else sync_timestamp


def get_filtered_amount_of_data(db_campaign, from_timestamp, till_timestamp, db_user=None, db_data_source=None):
	cur = get_db_connection().cursor(cursor_factory=psycopg2_extras.DictCursor)
	amount = 0

	if db_user is None:
		# all users
		if db_data_source is None:
			# all data sources
			for db_participant_user in get_campaign_participants(db_campaign=db_campaign):
				cur.execute(f'select count(*) as "amount" from "data"."{db_campaign["id"]}-{db_participant_user["id"]}" where "timestamp">=%s and "timestamp"<%s;', (
					from_timestamp,
					till_timestamp
				))
				amount += cur.fetchone()['amount']
		else:
			# single data source
			for db_participant_user in get_campaign_participants(db_campaign=db_campaign):
				cur.execute(f'select count(*) as "amount" from "data"."{db_campaign["id"]}-{db_participant_user["id"]}" where "data_source_id"=%s and "timestamp">=%s and "timestamp"<%s;', (
					db_data_source['id'],
					from_timestamp,
					till_timestamp
				))
				amount += cur.fetchone()['amount']
	else:
		# single user
		if db_data_source is None:
			# all data sources
			cur.execute(f'select count(*) as "amount" from "data"."{db_campaign["id"]}-{db_user["id"]}" where "timestamp">=%s and "timestamp"<%s;', (
				from_timestamp,
				till_timestamp
			))
			amount += cur.fetchone()['amount']
		else:
			# single data source
			# f'select count(*) as "amount" from "data"."{db_campaign["id"]}-{db_user["id"]}" where "data_source_id"={db_data_source["id"]} and "timestamp">={from_timestamp} and "timestamp"<{till_timestamp};'
			cur.execute(f'select count(*) as "amount" from "data"."{db_campaign["id"]}-{db_user["id"]}" where "data_source_id"=%s and "timestamp">=%s and "timestamp"<%s;', (
				db_data_source['id'],
				from_timestamp,
				till_timestamp
			))
			amount += cur.fetchone()['amount']
	cur.close()
	get_db_connection().commit()
	return amount

# endregion
