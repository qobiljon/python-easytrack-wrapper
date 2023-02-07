export PYTHONUNBUFFERED=1
export PIPENV_VERBOSITY=-1
export DJANGO_SETTINGS_MODULE=dashboard.settings

# check code style
echo 'Checking code style...'
pipenv run pylint src
read -p "Press [Enter] to continue..."

# run tests
echo 'Running unit tests...'
if ! pipenv run coverage run --source='.' -m unittest src.easytrack.tests; then
  echo 'Tests failed =('
  exit 1
fi

# generate coverage report
pipenv run coverage report
exit 0
