pipenv run python -c "from setuptools import setup; setup()" clean --all
pipenv run python setup.py sdist
pipenv run twine upload dist/*
