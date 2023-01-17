export PYTHONUNBUFFERED=1
export PIPENV_VERBOSITY=-1
export DJANGO_SETTINGS_MODULE=dashboard.settings

# exec pipenv run ./manage.py test
echo 'Running tests...'
if pipenv run coverage run --source='.' -m unittest src.easytrack.tests; then
  echo 'Tests passed =)'
  exec pipenv run coverage report
else
  echo 'Tests failed =('
fi
