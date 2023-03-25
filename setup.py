from setuptools import setup, find_packages

setup(
    name = 'python-easytrack',
    version = '1.1.15',
    license = 'MIT',
    author = "Kobiljon Toshnazarov",
    author_email = 'kobiljon.toshnazarov@gmail.com',
    packages = find_packages('src'),
    package_dir = {'': 'src'},
    url = 'https://github.com/easy-track/easytrack',
    keywords = 'easytrack boilerplate',
    install_requires = ['psycopg2-binary', 'peewee', 'python-dateutil', 'pytz'],
)
