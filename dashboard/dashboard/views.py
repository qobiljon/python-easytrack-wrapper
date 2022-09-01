from typing import List, Tuple, Optional, Dict
from datetime import timedelta as td
import plotly.graph_objects as go
import collections
import mimetypes
import datetime
import zipfile
import plotly
import json
import os
import re

# libs
from wsgiref.util import FileWrapper
from django.views.decorators.http import require_http_methods
from django.contrib.auth.decorators import login_required
from django.contrib.auth import logout as dj_logout
from django.contrib.auth import login as dj_login
from django.contrib.auth import authenticate as dj_authenticate
from django.contrib.auth.models import User
from django.http import StreamingHttpResponse
from django.shortcuts import render, redirect
from django.http import HttpResponse

# app
from dashboard.models import EnhancedDataSource
from boilerplate import selectors as slc, models
from boilerplate import services as svc
from boilerplate import utils


def handle_google_verification(request):
	return render(request=request, template_name='../templates/google43e44b3701ba10c8.html')


@require_http_methods(['GET', 'POST'])
def handle_login_api(request):
	if request.user.is_authenticated:
		user = slc.find_user(user_id=None, email=request.user.email)
		if user is None:
			print('new user : ', end='')
			session_key = utils.md5(value=f'{request.user.email}{utils.now_us()}')
			user = svc.create_user(
				name=request.user.get_full_name(),
				email=request.user.email,
				session_key=session_key
			)
			if user is None:
				dj_logout(request=request)
			else:
				return redirect(to='campaigns-list')
		else:
			return redirect(to='campaigns-list')
	return render(
		request=request,
		template_name='../templates/page_authentication.html',
		context={'title': 'Authentication'}
	)


@require_http_methods(['GET'])
def handle_development_login_api(request):
	dev_email = 'dev@easytrack.com'
	user = slc.find_user(
		user_id=None,
		email=request.user.email if request.user.is_authenticated else dev_email
	)
	if user is None:
		print('new user : ', end='')
		session_key = utils.md5(value=f'{dev_email}{utils.now_us()}')
		user = svc.create_user(
			name='Developer',
			email=dev_email,
			session_key=session_key
		)
		if user is None:
			dj_logout(request=request)
		else:
			if User.objects.filter(email=dev_email).exists():
				dj_user = User.objects.get(email=dev_email)
			else:
				dj_user = User.objects.create_user(
					username=dev_email,
					email=dev_email,
					password=dev_email,
					first_name='Developer'
				)
			if dj_authenticate(username=dev_email, password=dev_email):
				dj_login(
					request=request,
					user=dj_user,
					backend='django.contrib.auth.backends.ModelBackend'
				)
				return redirect(to='campaigns-list')
			else:
				return redirect(to='login')
	else:
		if User.objects.filter(email=dev_email).exists():
			dj_user = User.objects.get(email=dev_email)
		else:
			dj_user = User.objects.create_user(
				username=dev_email,
				email=dev_email,
				password=dev_email,
				first_name='Developer'
			)
			dj_user.save()
		if dj_authenticate(username=dev_email, password=dev_email):
			dj_login(
				request=request,
				user=dj_user,
				backend='django.contrib.auth.backends.ModelBackend'
			)
			return redirect(to='campaigns-list')
		else:
			return redirect(to='login')


@login_required
@require_http_methods(['GET', 'POST'])
def handle_logout_api(request):
	dj_logout(request=request)
	return redirect(to='login')


@login_required
@require_http_methods(['GET'])
def handle_campaigns_list(request):
	user = slc.find_user(
		user_id=None,
		email=request.user.email
	)
	if user is not None:
		campaigns = list()
		for c in slc.get_supervisor_campaigns(user=user):
			participantsCnt = slc.get_campaign_participants_count(campaign=c)
			campaigns.append({
				'id': c.id,
				'name': c.name,
				'participants': participantsCnt,
				'created_by_me': c.owner == user
			})
		print('%s opened the main page' % request.user.email)
		campaigns.sort(key=lambda x: x['id'])

		return render(
			request=request,
			template_name='../templates/page_campaigns.html',
			context={
				'title': "%s's campaigns" % request.user.get_full_name(),
				'my_campaigns': campaigns,
				'id': user.id,
				'session_key': user.session_key
			}
		)
	else:
		dj_logout(request=request)
		return redirect(to='login')


@login_required
@require_http_methods(['GET'])
def handle_participants_list(request):
	user = slc.find_user(user_id=None, email=request.user.email)
	if user is not None:
		if 'id' in request.GET and str(request.GET['id']).isdigit():
			campaign = slc.get_campaign(campaign_id=int(request.GET['id']))
			if campaign and slc.is_supervisor(user=user, campaign=campaign):
				# check if data is submitted by user or researchers
				data_sources = slc.get_campaign_data_sources(campaign=campaign)
				if len(data_sources) == 0:
					return redirect(to='https://drive.google.com/drive/folders/1rho3la0tfZI_YLp4Lkq8MwuLF7K9SNIS')
				else:
					# campaign easytrack page
					participants = list()
					for p in slc.get_campaign_participants(campaign=campaign):
						stats = slc.get_participants_latest_stats(participant=p)
						participants.append({
							'id': p.user.id,
							'name': p.user.name,
							'email': p.user.email,
							'day_no': stats.participation_duration,
							'amount_of_data': stats.amount_of_data,
							'last_heartbeat_time': p.last_heartbeat_ts,
							'last_sync_time': stats.last_sync_ts,
						})
					participants.sort(key=lambda x: x['id'])
					return render(
						request=request,
						template_name='../templates/page_campaign_participants.html',
						context={
							'title': "%s's participants" % campaign.name,
							'campaign': campaign,
							'participants': participants,
							'id': user.id,
							'session_key': user.session_key,
							'joined': slc.is_participant(user=user, campaign=campaign)
						}
					)
			else:
				return redirect(to='campaigns-list')
		else:
			return redirect(to='campaigns-list')
	else:
		dj_logout(request=request)
		return redirect(to='login')


@login_required
@require_http_methods(['GET'])
def handle_researchers_list(request):
	user = slc.find_user(user_id=None, email=request.user.email)
	if user is not None:
		if 'campaign_id' in request.GET and str(request.GET['campaign_id']).isdigit():
			campaign = slc.get_campaign(campaign_id=int(request.GET['campaign_id']))
			supervisor = slc.get_supervisor(user=user, campaign=campaign)
			if supervisor:
				if 'targetEmail' in request.GET and 'action' in request.GET and request.GET['action'] in ['add', 'remove']:
					targetUser = slc.find_user(
						user_id=None,
						email=request.GET['targetEmail']
					)
					if targetUser is not None:
						if request.GET['action'] == 'add':
							svc.add_supervisor_to_campaign(
								new_user=targetUser,
								supervisor=supervisor
							)
						elif request.GET['action'] == 'remove':
							oldSupervisor = slc.get_supervisor(
								user=targetUser,
								campaign=campaign
							)
							if oldSupervisor is not None and oldSupervisor.user != campaign.owner:
								svc.remove_supervisor_from_campaign(
									oldSupervisor=oldSupervisor
								)
							else:
								return redirect(to='campaigns-list')
						else:
							return redirect(to='campaigns-list')

						# return new list of campaign's supervisors
						supervisors = list()
						for s in slc.get_campaign_supervisors(campaign=campaign):
							supervisors.append({
								'name': s.user.name,
								'email': s.user.email
							})
						supervisors.sort(key=lambda x: x['name'])

						return render(
							request=request,
							template_name='../templates/page_campaign_researchers.html',
							context={
								'title': "%s's researchers" % campaign.name,
								'campaign': campaign,
								'researchers': supervisors,
								'id': user.id,
								'session_key': user.session_key
							}
						)
					else:
						return redirect(to='campaigns-list')
				else:
					return redirect(to='campaigns-list')
			else:
				return redirect(to='campaigns-list')
		else:
			return redirect(to='campaigns-list')
	else:
		dj_logout(request=request)
		return redirect(to='login')


@login_required
@require_http_methods(['GET'])
def handle_participants_data_list(request):
	user = slc.find_user(user_id=None, email=request.user.email)
	if user is not None:
		if 'campaign_id' in request.GET and str(request.GET['campaign_id']).isdigit():
			campaign = slc.get_campaign(campaign_id=int(request.GET['campaign_id']))
			if campaign and slc.is_supervisor(user=user, campaign=campaign):
				if 'participant_id' in request.GET and utils.is_numeric(request.GET['participant_id']):
					participant_user = slc.find_user(user_id=int(request.GET['participant_id']), email=None)
					participant = None
					if participant_user:
						participant = slc.get_participant(
							user=participant_user,
							campaign=campaign
						)
					if participant is not None:
						data_sources = list()
						stats = slc.get_participants_latest_stats(participant=participant)
						for data_source in slc.get_campaign_data_sources(campaign=campaign):
							data_source_stats = stats[data_source]
							data_sources.append({
								'id': data_source.id,
								'name': data_source.name,
								'icon_name': data_source.icon_name,
								'amount_of_data': data_source_stats.amount_of_samples,
								'last_sync_time': utils.ts2str(ts=data_source_stats.last_sync_time)
							})
						data_sources.sort(key=lambda x: x['name'])
						return render(
							request=request,
							template_name='../templates/page_participant_data_sources_stats.html',
							context={
								'title': f'Data submitted by {participant_user.email} (ID = {participant_user.id})',
								'campaign': campaign,
								'participant': participant_user,
								'data_sources': data_sources,
								'id': user.id,
								'session_key': user.session_key
							}
						)
					else:
						return redirect(to='campaigns-list')
				else:
					return redirect(to='campaigns-list')
			else:
				return redirect(to='campaigns-list')
		else:
			return redirect(to='campaigns-list')
	else:
		dj_logout(request=request)
		return redirect(to='login')


@login_required
@require_http_methods(['GET'])
def dev_join_campaign(request):
	user = slc.find_user(user_id=None, email=request.user.email)
	if user is not None:
		if 'campaign_id' in request.GET and str(request.GET['campaign_id']).isdigit():
			campaign = slc.get_campaign(campaign_id=int(request.GET['campaign_id']))
			if campaign and slc.is_supervisor(user=user, campaign=campaign):
				svc.add_participant_to_campaign(add_user=user, campaign=campaign)
				return redirect(to='campaigns-list')
			else:
				return redirect(to='campaigns-list')
		else:
			return redirect(to='campaigns-list')
	else:
		dj_logout(request=request)
		return redirect(to='login')


@login_required
@require_http_methods(['GET'])
def handle_raw_samples_list(request):
	user = slc.find_user(user_id=None, email=request.user.email)
	if user is not None:
		if 'campaign_id' in request.GET and str(request.GET['campaign_id']).isdigit():
			campaign = slc.get_campaign(campaign_id=int(request.GET['campaign_id']))
			if campaign and slc.is_supervisor(user=user, campaign=campaign):
				if 'email' in request.GET:
					db_participant_user = slc.find_user(user_id=None, email=request.GET['email'])
					if db_participant_user is not None and slc.is_participant(user=db_participant_user, campaign=campaign) and \
						'from_timestamp' in request.GET and 'data_source_id' in request.GET and utils.is_numeric(request.GET['from_timestamp']) and utils.is_numeric(request.GET['data_source_id']):
						participant = slc.get_participant(user=user, campaign=campaign)
						from_timestamp = utils.int2ts(int(request.GET['from_timestamp']))
						data_source = slc.find_data_source(data_source_id=int(request.GET['data_source_id']), name=None)
						if data_source is not None:
							records = []
							for i, record in enumerate(slc.get_next_k_data_records(
								participant=participant,
								data_source=data_source,
								from_ts=from_timestamp, k=500
							)):
								value = json.dumps(record.val)
								if len(value) > 5 * 1024:  # 5KB (e.g., binary files)
									value = f'[ {len(value):,} byte data record ]'
								records += [{
									'row': i + 1,
									'timestamp': utils.ts2str(ts=record.ts),
									'value': value
								}]
								from_timestamp = record.ts
							return render(
								request=request,
								template_name='../templates/page_raw_data_view.html',
								context={
									'title': data_source.name,
									'records': records,
									'from_timestamp': utils.ts2int(from_timestamp),
									'id': user.id,
									'session_key': user.session_key
								}
							)
						else:
							return redirect(to='campaigns-list')
					else:
						return redirect(to='campaigns-list')
				else:
					return redirect(to='campaigns-list')
			else:
				return redirect(to='campaigns-list')
		else:
			return redirect(to='campaigns-list')
	else:
		dj_logout(request=request)
		return redirect(to='login')


@login_required
@require_http_methods(['GET', 'POST'])
def handle_campaign_editor(request):
	user = slc.find_user(user_id=None, email=request.user.email)
	if user is not None:
		if request.method == 'GET':
			# request to open the campaign editor
			db_data_sources = slc.get_all_data_sources()
			if 'edit' in request.GET and 'campaign_id' in request.GET and str(request.GET['campaign_id']).isdigit():
				# edit an existing campaign
				campaign = slc.get_campaign(campaign_id=int(request.GET['campaign_id']))
				if campaign and slc.is_supervisor(user=user, campaign=campaign):
					data_source_infos: List[Dict[str, str]] = list()
					selected_data_sources = set(slc.get_campaign_data_sources(campaign=campaign))
					for data_source in slc.get_all_data_sources():
						data_source_infos.append({
							'name': data_source.name,
							'icon_name': data_source.icon_name,
							'selected': data_source in selected_data_sources
						})
					data_source_infos.sort(key=lambda key: key['name'])
					return render(
						request=request,
						template_name='../templates/page_campaign_editor.html',
						context={
							'edit_mode': True,
							'title': '"%s" Campaign Editor' % campaign.name,
							'campaign': campaign,
							'campaign_start_time': utils.ts2web(ts=campaign.start_ts),
							'campaign_end_time': utils.ts2web(ts=campaign.end_ts),
							'data_sources': data_source_infos
						}
					)
				else:
					return redirect(to='campaigns-list')
			else:
				# edit for a new campaign
				data_sources = []
				for data_source in db_data_sources:
					data_sources += [{
						'name': data_source.name,
						'icon_name': data_source.iconName
					}]
				data_sources.sort(key=lambda key: key['name'])
				return render(
					request=request,
					template_name='../templates/page_campaign_editor.html',
					context={
						'title': 'New campaign',
						'data_source': data_sources,
					}
				)
		elif request.method == 'POST':
			campaign: Optional[models.Campaign] = None
			if 'campaign_id' in request.POST and utils.is_numeric(request.POST['campaign_id']) and int(request.POST['campaign_id']) > -1:
				campaign = slc.get_campaign(campaign_id=int(request.POST['campaign_id']))
				if not campaign or not slc.is_supervisor(user=user, campaign=campaign): return redirect(to='campaigns-list')

			if 'name' in request.POST and all(map(lambda s: s in request.POST and utils.is_web_ts(request.POST[s]), ['startTime', 'endTime'])):
				data_source_names = map(
					lambda s: request.POST[s],
					filter(lambda s: re.fullmatch(r'NEW_DATA_SOURCE_\d+', s), request.POST)
				)
				data_sources: List[models.DataSource] = list()
				for name in data_source_names:
					icon = request.POST[f'icon_name_{name}']
					data_sources.append(svc.create_data_source(
						name=name,
						icon_name=icon
					))
				campaign_name = str(request.POST['name'])
				campaign_start_ts = utils.parse_ts(request.POST['startTime'])
				campaign_end_ts = utils.parse_ts(request.POST['endTime'])

				if campaign and slc.is_supervisor(user=user, campaign=campaign):
					svc.update_campaign(
						supervisor=slc.get_supervisor(user=user, campaign=campaign),
						name=campaign_name,
						start_ts=campaign_start_ts,
						end_ts=campaign_end_ts,
						data_sources=data_sources
					)
				elif not campaign:
					svc.create_campaign(
						owner=user,
						name=campaign_name,
						start_ts=campaign_start_ts,
						end_ts=campaign_end_ts,
						data_sources=data_sources
					)
				return redirect(to='campaigns-list')
			else:
				return redirect(to='campaigns-list')
		else:
			return redirect(to='campaigns-list')
	else:
		dj_logout(request=request)
		return redirect(to='login')


@login_required
@require_http_methods(['GET'])
def handle_easytrack_monitor(request):
	user = slc.find_user(user_id=None, email=request.user.email)
	if user is not None:
		if 'campaign_id' in request.GET and utils.is_numeric(request.GET['campaign_id']):
			campaign = slc.get_campaign(campaign_id=int(request.GET['campaign_id']))
			all_data_sources = slc.get_campaign_data_sources(campaign=campaign)
			all_participants = slc.get_campaign_participants(campaign=campaign)
			selected_participant = None
			selected_data_source = None
			if campaign is not None:
				from_ts = datetime.datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
				till_ts = from_ts + datetime.timedelta(hours=24)

				if 'plot_date' in request.GET:
					plot_date_str = str(request.GET['plot_date'])
					if re.search(r'\d{4}-\d{1,2}-\d{1,2}', plot_date_str) is not None:
						year, month, day = plot_date_str.split('-')
						from_ts = datetime.datetime(year=int(year), month=int(month), day=int(day), hour=0, minute=0, second=0, microsecond=0)
						till_ts = from_ts + datetime.timedelta(hours=24)

				if 'participant_id' in request.GET and utils.is_numeric(request.GET['participant_id']):
					selected_user = slc.find_user(user_id=int(request.GET['participant_id']), email=None)
					if selected_user is not None:
						selected_participant = slc.get_participant(user=selected_user, campaign=campaign)

				WINDOW_SIZE = td(hours=1)  # 1-hour sliding window
				if 'data_source_name' in request.GET:
					data_source_name = request.GET['data_source_name']
					if data_source_name == 'all':
						hourly_stats = collections.defaultdict(int)
						# region compute hourly stats
						for participant in (all_participants if selected_participant is None else [selected_participant]):
							for data_source in all_data_sources:
								ts = from_ts
								while ts < till_ts:
									amount = slc.get_filtered_amount_of_data(
										participant=participant,
										data_source=data_source,
										from_ts=ts,
										till_ts=ts + WINDOW_SIZE
									)
									hourly_stats[ts.hour] += amount
									ts += WINDOW_SIZE
						# endregion

						plot_data_source = {'name': 'all campaign data sources combined'}
						# region plot hourly stats
						x = []
						y = []
						max_amount = 10
						hours = list(hourly_stats.keys())
						hours.sort()
						for hour in hours:
							amount = hourly_stats[hour]
							if hour < 13:
								hour = f'{hour} {"pm" if hour == 12 else "am"}'
							else:
								hour = f'{hour % 12} pm'
							x += [hour]
							y += [amount]
							max_amount = max(max_amount, amount)
						fig = go.Figure([go.Bar(x=x, y=y)])
						fig.update_yaxes(range=[0, max_amount])
						plot_str = plotly.offline.plot(fig, auto_open=False, output_type="div")
						plot_data_source['plot'] = plot_str
						# endregion

						return render(
							request=request,
							template_name='../templates/easytrack_monitor.html',
							context={
								'title': 'EasyTracker',
								'campaign': campaign,
								'plot_date': f'{from_ts.year}-{from_ts.month:02}-{from_ts.day:02}',

								'participants': all_participants,
								'plot_participant': selected_participant,

								'all_data_sources': all_data_sources,
								'plot_data_source': plot_data_source
							}
						)
					else:
						data_source = slc.find_data_source(data_source_id=None, name=data_source_name)
						if data_source is not None:
							hourly_stats = collections.defaultdict(int)
							# region compute hourly stats
							for participant in (all_participants if selected_participant.id == 'all' else [selected_participant]):
								ts = from_ts
								while ts < till_ts:
									amount = slc.get_filtered_amount_of_data(
										participant=participant,
										data_source=data_source,
										from_ts=ts,
										till_ts=ts + WINDOW_SIZE
									)
									hourly_stats[ts.hour] += amount
									ts += WINDOW_SIZE
							# endregion

							plot_data_source = EnhancedDataSource(db_data_source=data_source)
							# region plot hourly stats
							x = []
							y = []
							max_amount = 10
							hours = list(hourly_stats.keys())
							hours.sort()
							for hour in hours:
								amount = hourly_stats[hour]
								if hour < 13:
									hour = f'{hour} {"pm" if hour == 12 else "am"}'
								else:
									hour = f'{hour % 12} pm'
								x += [hour]
								y += [amount]
								max_amount = max(max_amount, amount)
							fig = go.Figure([go.Bar(x=x, y=y)])
							fig.update_yaxes(range=[0, max_amount])
							plot_str = plotly.offline.plot(fig, auto_open=False, output_type="div")
							plot_data_source.attach_plot(plot_str=plot_str)
							# endregion

							return render(
								request=request,
								template_name='../templates/easytrack_monitor.html',
								context={
									'title': 'EasyTracker',
									'campaign': campaign,
									'plot_date': f'{from_ts.year}-{from_ts.month:02}-{from_ts.day:02}',

									'participants': all_participants,
									'plot_participant': selected_participant,

									'all_data_sources': all_data_sources,
									'plot_data_source': plot_data_source
								}
							)
						else:
							return redirect(to='campaigns-list')
				else:
					return redirect(to='campaigns-list')
			else:
				return redirect(to='campaigns-list')
		else:
			return redirect(to='campaigns-list')
	else:
		dj_logout(request=request)
		return redirect(to='login')


@login_required
@require_http_methods(['GET'])
def handle_dataset_info(request):
	user = slc.find_user(user_id=None, email=request.user.email)
	if user is not None:
		if 'campaign_id' in request.GET and utils.is_numeric(request.GET['campaign_id']):
			campaign = slc.get_campaign(campaign_id=int(request.GET['campaign_id']))
			if campaign is not None:
				campaign_data_sources = slc.get_campaign_data_sources(campaign=campaign)
				campaign_data_sources.sort(key=lambda x: x.name)
				db_participants = list(slc.get_campaign_participants(campaign=campaign))
				db_participants.sort(key=lambda db_participant: db_participant.id)
				return render(
					request=request,
					template_name='../templates/page_dataset_configs.html',
					context={
						'campaign': campaign,
						'data_sources': campaign_data_sources,
						'participants': db_participants,
						'id': user.id,
						'session_key': user.session_key
					}
				)
			else:
				return redirect(to='campaigns-list')
		else:
			return redirect(to='campaigns-list')
	else:
		dj_logout(request=request)
		return redirect(to='login')


@login_required
@require_http_methods(['GET'])
def handle_delete_campaign_api(request):
	user = slc.find_user(user_id=None, email=request.user.email)
	if user is not None:
		if 'campaign_id' in request.GET and utils.is_numeric(request.GET['campaign_id']):
			campaign = slc.get_campaign(campaign_id=int(request.GET['campaign_id']))
			if campaign and slc.is_supervisor(user=user, campaign=campaign):
				svc.delete_campaign(supervisor=slc.get_supervisor(user=user, campaign=campaign))
				return redirect(to='campaigns-list')
			else:
				return redirect(to='campaigns-list')
		else:
			return redirect(to='campaigns-list')
	else:
		dj_logout(request=request)
		return redirect(to='login')


@login_required
@require_http_methods(['GET'])
def handle_download_data_api(request):
	user = slc.find_user(user_id=None, email=request.user.email)
	if user is not None:
		if 'campaign_id' in request.GET and utils.is_numeric(request.GET['campaign_id']):
			campaign = slc.get_campaign(campaign_id=int(request.GET['campaign_id']))
			if campaign and slc.is_supervisor(user=user, campaign=campaign):
				if 'participant_id' in request.GET and utils.is_numeric(request.GET['participant_id']):
					target_user = slc.find_user(user_id=int(request.GET['participant_id']), email=None)
					if target_user is not None and slc.is_participant(user=target_user, campaign=campaign):
						# dump data data
						dump_filepath = svc.dump_data(
							participant=slc.get_participant(user=target_user, campaign=campaign),
							data_source=None
						)
						print(f'dump filepath : {dump_filepath}')
						with open(dump_filepath, 'rb') as r:
							dump_content = bytes(r.read())
						os.remove(dump_filepath)

						# archive the dump content
						now = datetime.datetime.now()
						filename = f'easytrack-data-{target_user.email}-{now.month}-{now.day}-{now.year} {now.hour}-{now.minute}.zip'
						file_path = utils.get_temp_filepath(filename=filename)
						fp = zipfile.ZipFile(file_path, 'w', zipfile.ZIP_STORED)
						fp.writestr(f'{target_user.email}.csv', dump_content)
						fp.close()
						with open(file_path, 'rb') as r:
							content = r.read()
						os.remove(file_path)

						res = HttpResponse(content=content, content_type='application/x-binary')
						res['Content-Disposition'] = f'attachment; filename={filename}'
						return res
					else:
						return redirect(to='campaigns-list')
				else:
					return redirect(to='campaigns-list')
			else:
				return redirect(to='campaigns-list')
		else:
			return redirect(to='campaigns-list')
	else:
		dj_logout(request=request)
		return redirect(to='login')


@login_required
@require_http_methods(['GET'])
def handle_download_csv_api(request):
	user = slc.find_user(user_id=None, email=request.user.email)
	if user is not None:
		if 'campaign_id' in request.GET and utils.is_numeric(request.GET['campaign_id']):
			campaign = slc.get_campaign(campaign_id=int(request.GET['campaign_id']))
			if campaign and slc.is_supervisor(user=user, campaign=campaign):
				if 'user_id' in request.GET and utils.is_numeric(request.GET['user_id']):
					target_user = slc.find_user(user_id=int(request.GET['user_id']), email=None)
					if target_user is not None and slc.is_participant(user=user, campaign=campaign):
						dump_filepath = svc.dump_data(
							participant=slc.get_participant(user=user, campaign=campaign),
							data_source=None
						)
					else:
						return redirect(to='campaigns-list')
				elif 'data_source_id' in request.GET and utils.is_numeric(request.GET['data_source_id']):
					data_source = slc.find_data_source(data_source_id=int(request.GET['data_source_id']), name=None)
					dump_filepaths: List[Tuple[models.Participant, str]] = list()
					if data_source:
						for participant in slc.get_campaign_participants(campaign=campaign):
							dump_filepaths.append((
								participant,
								svc.dump_data(participant=participant, data_source=data_source)
							))

						# archive the dump content
						now = datetime.datetime.now()
						filename = f'easytrack-data-{data_source.name}-{now.month}-{now.day}-{now.year} {now.hour}-{now.minute}.zip'
						dump_filepath = utils.get_temp_filepath(filename=filename)
						print(f'dump filepath : {dump_filepath}')
						fp = zipfile.ZipFile(dump_filepath, 'w', compression=zipfile.ZIP_DEFLATED, compresslevel=9)
						for participant, csv_filepath in dump_filepaths:
							with open(csv_filepath, 'rb') as r:
								fp.writestr(
									zinfo_or_arcname=f'{participant.user.email}.csv',
									data=bytes(r.read())
								)
							os.remove(dump_filepath)
						fp.close()
					else:
						return redirect(to='campaigns-list')
				else:
					dump_filepaths: List[Tuple[models.Participant, str]] = list()
					for participant in slc.get_campaign_participants(campaign=campaign):
						dump_filepaths.append((
							participant,
							svc.dump_data(participant=participant, data_source=None)
						))

					# archive the dump content
					now = datetime.datetime.now()
					filename = f'easytrack-data-{now.month}-{now.day}-{now.year} {now.hour}-{now.minute}.zip'
					dump_filepath = utils.get_temp_filepath(filename=filename)
					print(f'dump filepath : {dump_filepath}')
					fp = zipfile.ZipFile(dump_filepath, 'w', compression=zipfile.ZIP_DEFLATED, compresslevel=9)
					for participant, csv_filepath in dump_filepaths:
						with open(csv_filepath, 'rb') as r:
							fp.writestr(
								zinfo_or_arcname=f'{participant.user.email}.csv',
								data=bytes(r.read())
							)
						os.remove(dump_filepath)
					fp.close()

				filename = os.path.basename(dump_filepath)
				chunk_size = 8192
				res = StreamingHttpResponse(
					streaming_content=FileWrapper(open(dump_filepath, 'rb'), chunk_size),
					content_type=mimetypes.guess_type(dump_filepath)[0],
				)
				res['Content-Length'] = os.path.getsize(dump_filepath)
				res['Content-Disposition'] = f'attachment; filename={filename}'
				return res
			else:
				return redirect(to='campaigns-list')
		else:
			return redirect(to='campaigns-list')
	else:
		dj_logout(request=request)
		return redirect(to='login')


@login_required
@require_http_methods(['GET'])
def handle_download_dataset_api(request):
	user = slc.find_user(user_id=None, email=request.user.email)
	if user is not None:
		if 'campaign_id' in request.GET and utils.is_numeric(request.GET['campaign_id']):
			campaign = slc.get_campaign(campaign_id=int(request.GET['campaign_id']))
			if campaign and slc.is_supervisor(user=user, campaign=campaign):
				dump_filepaths: List[Tuple[models.Participant, str]] = list()
				for participant in slc.get_campaign_participants(campaign=campaign):
					dump_filepaths.append((
						participant,
						svc.dump_data(participant=participant, data_source=None)
					))

				# archive the dump content
				now = datetime.datetime.now()
				filename = f'easytrack-data-{now.month}-{now.day}-{now.year} {now.hour}-{now.minute}.zip'
				dump_filepath = utils.get_temp_filepath(filename=filename)
				print(f'dump filepath : {dump_filepath}')
				fp = zipfile.ZipFile(dump_filepath, 'w', compression=zipfile.ZIP_DEFLATED, compresslevel=9)
				for participant, csv_filepath in dump_filepaths:
					with open(csv_filepath, 'rb') as r:
						fp.writestr(
							zinfo_or_arcname=f'{participant.user.email}.csv',
							data=bytes(r.read())
						)
					os.remove(dump_filepath)
				fp.close()

				filename = os.path.basename(dump_filepath)
				chunk_size = 8192
				res = StreamingHttpResponse(
					streaming_content=FileWrapper(open(dump_filepath, 'rb'), chunk_size),
					content_type=mimetypes.guess_type(dump_filepath)[0],
				)
				res['Content-Length'] = os.path.getsize(dump_filepath)
				res['Content-Disposition'] = f'attachment; filename={filename}'
				return res
			else:
				return redirect(to='campaigns-list')
		else:
			return redirect(to='campaigns-list')
	else:
		dj_logout(request=request)
		return redirect(to='login')
