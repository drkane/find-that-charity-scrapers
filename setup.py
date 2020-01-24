from setuptools import setup, find_packages

setup(
    name="findthatcharity_import",
    packages=find_packages(),
    version="0.0.2",
    author="David Kane david@dkane.net",
    include_package_data=True,
    license="MIT",
    description="Scrapers for findthatcharity",
    install_requires=[
        "alembic==1.3.3",
        "Scrapy==1.8.0",
        "validators==0.14.1",
        "titlecase==0.12.0",
        "tqdm==4.41.1",
        "openpyxl==3.0.3",
        "SQLAlchemy==1.3.13",
        "psycopg2==2.8.4",
        "psycopg2-binary==2.8.4",
        "pyexcel-ezodf==0.3.4",
        "pyexcel-io==0.5.20",
        "pyexcel-ods3==0.5.3",
        "pymongo==3.10.1",
        "redis==3.3.11",
        "bcp-reader==0.1.1",
    ],
    entry_points={
        'scrapy': [
            'settings = findthatcharity_import.settings'
        ]
    },
)