export PYTHONUNBUFFERED=1
export PIPENV_VERBOSITY=-1
export DJANGO_SETTINGS_MODULE=dashboard.settings

# exec pipenv run ./manage.py test
echo 'Running tests...'
if pipenv run coverage run --source='.' -m unittest src.easytrack.tests; then
  echo 'Tests passed, running pylint...'
  
  if pipenv run pylint src; then
    echo 'Pylint passed, running coverage report...'
    exec pipenv run coverage report
  else
    echo 'Pylint failed =('
  fi

else
  echo 'Tests failed =('
fi
