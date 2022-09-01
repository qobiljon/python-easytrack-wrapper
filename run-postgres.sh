# load environment
export $(cat .env | xargs)

# run postgres
docker run -d \
  --name db \
  -e POSTGRES_USER="$DATABASE_USER" \
  -e POSTGRES_PASSWORD="$DATABASE_PASSWORD" \
  -e POSTGRES_DB="$DATABASE_NAME" \
  -v pg_data:/var/lib/postgresql/data \
  --rm -P -p 127.0.0.1:5432:5432 \
  postgres:alpine
