# Custom as metabase/metabase has linking issues with libstdc++ (which DuckDB requires)
# `apk add --no-cache libstdc++` with the metabase/metabase does not work
# See: https://github.com/AlexR2D2/metabase_duckdb_driver/issues/3#issuecomment-1268768703
FROM openjdk:19-buster

ENV MB_PLUGINS_DIR=/home/plugins/

ADD https://downloads.metabase.com/v0.44.4/metabase.jar /home
ADD  https://github.com/AlexR2D2/metabase_duckdb_driver/releases/download/0.2.3/duckdb.metabase-driver.jar /home/plugins/
RUN chmod 744 /home/plugins/duckdb.metabase-driver.jar


CMD ["java", "-jar", "/home/metabase.jar"]