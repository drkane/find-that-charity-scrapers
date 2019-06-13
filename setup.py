from setuptools import setup

setup(
    name="findthatcharity_import",
    packages=["findthatcharity_import"],
    version="0.0.1",
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
    ]
)
