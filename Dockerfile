# syntax=docker/dockerfile:1.6
# https://hub.docker.com/r/docker/dockerfile
FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE="1" \ 
  PYTHONUNBUFFERED=1 \
  RYE_HOME="/opt/rye" \
  PATH="/opt/rye/shims:$PATH"

RUN apt-get update && apt-get install -y curl

RUN curl -sSf https://rye-up.com/get | RYE_NO_AUTO_INSTALL="1" RYE_INSTALL_OPTION="--yes" bash


COPY --link quickstart_etl /app/quickstart_etl/
COPY --link pagila_dbt/ /app/dbt/
COPY --link pyproject.toml requirements.lock README.md .python-version /app/


WORKDIR /app
RUN --mount=type=cache,target=/root/.cache \
  rye sync --no-dev --no-lock

EXPOSE 4266

# Dagster's Helm chart passes the full command, rather than just args.
# ENTRYPOINT ["dagster", "api", "grpc", "--module-name=quickstart_etl", "--port=4266"]