if [ $# -eq 0 ]
  then
    exec pipenv run pylint src
  else
    exec pipenv run pylint $1
fi
