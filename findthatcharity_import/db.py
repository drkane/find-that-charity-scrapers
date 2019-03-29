from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.sql.expression import Insert
from sqlalchemy import MetaData, Column, BigInteger, Integer, String, Date, DateTime, Boolean, Text, Table, Float, ForeignKey
import datetime

Base = declarative_base()
metadata = MetaData()

tables = {}

tables["organisation"] = Table('organisation', metadata, 
    Column("id", String(255), primary_key=True),
    Column("name", String(255)),
    Column("charityNumber", String(255)),
    Column("companyNumber", String(255)),
    Column("addressLocality", String(255)),
    Column("addressRegion", String(255)),
    Column("addressCountry", String(255)),
    Column("postalCode", String(255)),
    Column("telephone", String(255)),
    Column("email", String(255)),
    Column("description", Text),
    Column("url", String(255)),
    Column("latestIncome", BigInteger),
    Column("latestIncomeDate", Date),
    Column("dateRegistered", Date),
    Column("dateRemoved", Date),
    Column("active", Boolean),
    Column("status", String(255)),
    Column("parent", String(255)),
    Column("dateModified", DateTime),
)

tables["location"] = Table('location', metadata,
    Column("id", String(255), primary_key = True),
    Column("name", String(255)),
    Column("countryCode", String(2)),
    Column("latitude", Float),
    Column("longitude", Float),
    Column("description", Text),
    Column("geoCode", String(255)),
    Column("geoCodeType", String(255)),
    Column("dateModified", DateTime),
)

tables["source"] = Table('source', metadata,
    Column("identifier", String(255), primary_key = True),
    Column("title", String(255)),
    Column("description", Text),
    Column("license", String(255)),
    Column("license_name", String(255)),
    Column("issued", DateTime),
    Column("modified", DateTime),
    Column("publisher_name", String(255)),
    Column("publisher_website", String(255)),
)

tables["distribution"] = Table('distribution', metadata,
    Column('source_id', String(255), ForeignKey("source.identifier"), primary_key = True),
    Column("title", String(255), primary_key = True),
    Column("downloadURL", String(255)),
    Column("accessURL", String(255)),
)

tables["organisation_locations"] = Table('organisation_locations', metadata,
    Column('organisation_id', String(255), ForeignKey("organisation.id"), primary_key = True),
    Column('location_id', String(255), ForeignKey("location.id"), primary_key = True),
)

tables["orgids"] = Table('orgids', metadata,
    Column("id", String(255), primary_key = True),
    Column('organisation_id', String(255), ForeignKey("organisation.id"), primary_key = True),
)

tables["organisation_sources"] = Table('organisation_sources', metadata,
    Column('organisation_id', String(255), ForeignKey("organisation.id"), primary_key = True),
    Column('source_id', String(255), ForeignKey("source.identifier"), primary_key = True),
)

tables["organisation_names"] = Table('organisation_names', metadata,
    Column('organisation_id', String(255), ForeignKey("organisation.id"), primary_key = True),
    Column("name", String(255), primary_key = True),
)

tables["organisation_types"] = Table('organisation_types', metadata,
    Column('organisation_id', String(255), ForeignKey("organisation.id"), primary_key = True),
    Column("organisationType", String(255), primary_key = True),
)