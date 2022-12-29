export PYTHONUNBUFFERED=1
export PIPENV_VERBOSITY=-1

echo 'Test goes first! running tests...'
if pipenv run python -m unittest './src/easytrack/tests.py'; then
  echo 'Tests passed =)'
  echo 'Deploying...'

  pipenv run python -c "from setuptools import setup; setup()" clean --all
  pipenv run python setup.py sdist
  pipenv run twine upload dist/*
else
  echo 'Tests failed =('
fi
