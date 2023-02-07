export PYTHONUNBUFFERED=1
export PIPENV_VERBOSITY=-1

if ! ./test.sh; then
  exit 1
fi

read -p "Press [Enter] to continue deploying..."
pipenv run python -c "from setuptools import setup; setup()" clean --all
pipenv run python setup.py sdist
pipenv run twine upload dist/*
