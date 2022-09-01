# load environment
export $(cat .env | xargs)

# build and launch app
docker build . -t dashboard
docker run dashboard -p 80:80 -d
