#!/usr/bin/env bash

# Run functional tests (postgres, api)
# Requirements:
# - WM_ENV:   required - environment variable, one of the choices: dev, fat

export WM_ENV=${1:-dev}

# use ini file to load db connection
source /dev/stdin <<<"$(sed -n '/host/,/^$/p' config.${WM_ENV}.ini )"

echo "Copying data from ${WM_ENV} db.."
pg_dump --no-owner --schema=public --file="./dump-${WM_ENV}.sql" \
--dbname=postgresql://${user}:${password}@${host}:${port}/${database}
grep -v "CREATE SCHEMA public;" ./dump-${WM_ENV}.sql > temp && mv temp ./dump-${WM_ENV}.sql
echo "Data has been copied to file dump-${WM_ENV}.sql"

docker-compose -f ./docker-compose-${WM_ENV}.yml rm -s -f

echo "Starting services.."
docker-compose -f ./docker-compose-${WM_ENV}.yml pull
docker-compose -f ./docker-compose-${WM_ENV}.yml up --build



