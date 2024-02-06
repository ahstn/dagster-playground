#!/usr/bin/env bash

echo "1 - Applying DB migrations"
superset db upgrade

echo "2 - Setting up admin user"
superset fab create-admin \
              --username admin \
              --firstname Superset \
              --lastname Admin \
              --email admin@superset.com \
              --password admin

echo "3 - Setting up roles and perms"
superset init

echo "4 - Loading examples"
superset load_examples

echo "5 - Setting up DBs"
superset set_database_uri -d duckdb -u duckdb:///superset_home/db/database.duckdb