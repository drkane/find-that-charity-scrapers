from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.sql.expression import Insert
from sqlalchemy import MetaData, Column, BigInteger, Integer, String, Date, DateTime, Boolean, Text, Table, Float, ForeignKey, JSON
import datetime

Base = declarative_base()
metadata = MetaData()

tables = {}

tables["organisation"] = Table('organisation', metadata, 
    Column("id", String, primary_key=True),
    Column("name", String),
    Column("charityNumber", String),
    Column("companyNumber", String),
    Column("addressLocality", String),
    Column("addressRegion", String),
    Column("addressCountry", String),
    Column("postalCode", String),
    Column("telephone", String),
    Column("email", String),
    Column("description", Text),
    Column("url", String),
    Column("latestIncome", BigInteger),
    Column("latestIncomeDate", Date),
    Column("dateRegistered", Date),
    Column("dateRemoved", Date),
    Column("active", Boolean),
    Column("status", String),
    Column("parent", String),
    Column("dateModified", DateTime),
    Column("location", JSON),
    Column("orgIDs", JSON),
    Column("alternateName", JSON),
    Column("organisationType", JSON),
    Column("organisationTypePrimary", String),
    Column("source", String),
)

tables["source"] = Table('source', metadata,
    Column("identifier", String, primary_key = True),
    Column("title", String),
    Column("description", Text),
    Column("license", String),
    Column("license_name", String),
    Column("issued", DateTime),
    Column("modified", DateTime),
    Column("publisher_name", String),
    Column("publisher_website", String),
    Column("distribution", JSON),
)

tables["organisation_links"] = Table('organisation_links', metadata,
    Column("organisation_id_a", String, primary_key = True),
    Column('organisation_id_b', String, primary_key = True),
    Column('description', String),
    Column('source', String),
)
