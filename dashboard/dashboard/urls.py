from django.contrib.staticfiles.urls import staticfiles_urlpatterns
from django.urls import path, include
from django.contrib import admin
from dashboard import views

urlpatterns = [
	# authentication
	path('login/', views.handle_login_api, name='login'),
	path('logout/', views.handle_logout_api, name='logout'),
	path('dev_login/', views.handle_development_login_api, name='dev-login'),

	# easytrack navigation
	path('', views.handle_campaigns_list, name='campaigns-list'),
	path('campaign/', views.handle_participants_list, name='participants-list'),
	path('participant/', views.handle_participants_data_list, name='participant'),
	path('dev-join/', views.dev_join_campaign, name='dev-join-campaign'),
	path('data/', views.handle_raw_samples_list, name='view_data'),
	path('edit/', views.handle_campaign_editor, name='campaign-editor'),
	path('researchers/', views.handle_researchers_list, name='manage-researchers'),

	# API (e.g., download file)
	path('dataset-info/', views.handle_dataset_info, name='dataset-info'),
	path('download-dataset/', views.handle_download_dataset_api, name='download-dataset'),
	path('delete/', views.handle_delete_campaign_api, name='delete-campaign'),
	path('download-data/', views.handle_download_data_api, name='download-data'),
	path('download-csv/', views.handle_download_csv_api, name='download-csv'),

	# visuals (e.g., DQ)
	path('et-monitor/', views.handle_easytrack_monitor, name='easytrack-monitor'),

	# others
	path('admin/', admin.site.urls),
	path('google43e44b3701ba10c8.html', views.handle_google_verification),
	path('google-auth/', include('social_django.urls', namespace='social')),
]
urlpatterns += staticfiles_urlpatterns()
