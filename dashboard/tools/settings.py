import tempfile
import os

download_dir = os.path.join(tempfile.gettempdir(), 'easytrack_dashboard')
settings_dir = os.path.dirname(__file__)
PROJECT_ROOT = os.path.abspath(os.path.dirname(settings_dir))
STATIC_DIR = os.path.join(PROJECT_ROOT, 'static/')
db_conn = None
