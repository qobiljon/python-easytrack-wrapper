from dotenv import load_dotenv
import os

# .env file
load_dotenv()

DEBUG = True
ALLOWED_HOSTS = [
	'localhost',
	'127.0.0.1'
]
INTERNAL_IPS = [
	'127.0.0.1',
	'localhost',
	'172.17.0.1'
]

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

INSTALLED_APPS = [
	'django.contrib.admin',
	'django.contrib.auth',
	'django.contrib.sites',
	'django.contrib.contenttypes',
	'django.contrib.sessions',
	'django.contrib.messages',
	'django.contrib.staticfiles',
	'django.contrib.humanize',
	'social_django',
	'dashboard',
]
MIDDLEWARE = [
	'django.middleware.security.SecurityMiddleware',
	'django.contrib.sessions.middleware.SessionMiddleware',
	'django.middleware.common.CommonMiddleware',
	'django.middleware.csrf.CsrfViewMiddleware',
	'django.contrib.auth.middleware.AuthenticationMiddleware',
	'django.contrib.messages.middleware.MessageMiddleware',
	'django.middleware.clickjacking.XFrameOptionsMiddleware',
]
TEMPLATES = [
	{
		'BACKEND': 'django.template.backends.django.DjangoTemplates',
		'DIRS': [os.path.join(BASE_DIR, 'templates')],
		'APP_DIRS': True,
		'OPTIONS': {
			'context_processors': [
				'django.template.context_processors.debug',
				'django.template.context_processors.request',
				'django.contrib.auth.context_processors.auth',
				'django.contrib.messages.context_processors.messages',
			],
		},
	},
]
DATABASES = {
	'default': {
		'ENGINE': 'django.db.backends.postgresql_psycopg2',
		'HOST': os.getenv(key='DATABASE_HOST'),
		'PORT': int(os.getenv(key='DATABASE_PORT')),
		'NAME': os.getenv(key='DATABASE_NAME'),
		'USER': os.getenv(key='DATABASE_USER'),
		'PASSWORD': os.getenv(key='DATABASE_PASSWORD')
	}
}

LANGUAGE_CODE = 'en-us'
TIME_ZONE = 'Asia/Seoul'
USE_I18N = True
USE_L10N = True
USE_TZ = True

STATIC_ROOT = os.path.join(BASE_DIR, 'static')
STATIC_URL = '/static/'
STATICFILES_DIRS = (
	os.path.join(BASE_DIR, 'images', 'button'),
	os.path.join(BASE_DIR, 'images', 'data_source'),
	os.path.join(BASE_DIR, 'images', 'others'),
)

AUTHENTICATION_BACKENDS = (
	# 'social_core.backends.google.GoogleOpenId',
	'django.contrib.auth.backends.ModelBackend',
	'social_core.backends.google_openidconnect.GoogleOpenIdConnect',
	'social_core.backends.google.GoogleOAuth2',
)
SOCIAL_AUTH_GOOGLE_OAUTH2_KEY = "79296265957-khvficocpqmhajv3c5obiljo2k95jqt3.apps.googleusercontent.com"
SOCIAL_AUTH_GOOGLE_OAUTH2_SECRET = "u4SAx6SzM7vBwXYAYCW0OGLe"

LOGIN_URL = 'login'
LOGIN_REDIRECT_URL = 'login'
LOGOUT_REDIRECT_URL = LOGIN_REDIRECT_URL

SITE_ID = 1
SECRET_KEY = 'cnbtigdbatfgl5iw89bh*$-y4j@g!c)qtuwmmi=ld!d^-he3o)'
ROOT_URLCONF = 'dashboard.urls'
WSGI_APPLICATION = 'dashboard.wsgi.application'
