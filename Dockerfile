# syntax=docker/dockerfile:1.6
# https://hub.docker.com/r/docker/dockerfile
FROM python:3.11-slim

COPY --link quickstart_etl /app/quickstart_etl/
COPY --link dbt/ /app/dbt/
COPY --link setup.py setup.cfg /app/

WORKDIR /app
RUN --mount=type=cache,target=/root/.cache \
  pip install -e .

EXPOSE 4266

# Dagster's Helm chart passes the full command, rather than just args.
# ENTRYPOINT ["dagster", "api", "grpc", "--module-name=quickstart_etl", "--port=4266"]