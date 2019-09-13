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
        "Scrapy==1.5.1",
        "validators==0.12.2",
        "titlecase==0.12.0",
        "tqdm==4.26.0",
        "openpyxl==2.5.7",
        "SQLAlchemy==1.3.1",
        "pyexcel-ezodf==0.3.4",
        "pyexcel-io==0.5.9.1",
        "pyexcel-ods3==0.5.2",
        "psycopg2==2.8.3",
        "pymongo==3.7.1",
        "alembic==1.1.0",
    ]
)
