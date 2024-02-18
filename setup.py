from setuptools import find_packages, setup

setup(
    name="quickstart_etl",
    description="Playground for experimenting with Dagster",
    url="https://github.com/ahstn/dagster-playground",
    license="MIT",
    python_requires=">=3.10",
    packages=find_packages(exclude=["quickstart_etl_tests"]),
    install_requires=[
        "dagster>=1.6.3",
        "dagster-graphql>=1.6.3",
        "python-dotenv",
        "boto3",
        "pandas",
        "psycopg2==2.9.9",
        "psycopg2-binary==2.9.9",
        "matplotlib",
        "textblob",
        "tweepy",
        "wordcloud",
        "dagster-duckdb>=0.21.14",
        "dagster-duckdb-pandas>=0.21.14",
        "duckdb==0.9.2",
        "dagster-dbt>=0.22.3",
        "dbt-duckdb>=1.7.1",
        "dagster_deltalake",
        "dagster-deltalake-pandas",
        # Trino IO Manager
        "trino[sqlalchemy]",
        "fsspec",
        "s3fs",
        "pyarrow",
    ],
    extras_require={"dev": ["dagster-webserver", "pytest", "ruff>=0.2.1", "pandas-stubs"]},
)
