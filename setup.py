from setuptools import find_packages, setup

setup(
    name="quickstart_etl",
    description="Playground for experimenting with Dagster",
    url="https://github.com/ahstn/dagster-playground",
    license="MIT",
    python_requires=">=3.10",
    packages=find_packages(exclude=["quickstart_etl_tests"]),
    install_requires=[
        "dagster",
        "dagster-cloud",
        "python-dotenv",
        "boto3",
        "pandas",
        "psycopg2-binary==2.9.9",
        "matplotlib",
        "textblob",
        "tweepy",
        "wordcloud",

        "dagster-duckdb==0.21.13",
        "dagster-duckdb-pandas==0.21.13",
        "duckdb==0.9.2",
    ],
    extras_require={
        "dev": [
          "dagster-webserver", 
          "pytest", 
          "ruff",
          "pandas-stubs"
        ]
    },
)
