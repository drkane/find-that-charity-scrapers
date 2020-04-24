"""change json to jsonb

Revision ID: 66e2f2bd33e6
Revises: cff2f4e14057
Create Date: 2020-04-24 16:46:26.363414

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB


# revision identifiers, used by Alembic.
revision = '66e2f2bd33e6'
down_revision = 'cff2f4e14057'
branch_labels = None
depends_on = None


def upgrade():
    op.alter_column('organisation', 'location', type_=JSONB(), postgresql_using='"location"::jsonb')
    op.alter_column('organisation', 'orgIDs', type_=JSONB(), postgresql_using='"orgIDs"::jsonb')
    op.alter_column('organisation', 'alternateName', type_=JSONB(), postgresql_using='"alternateName"::jsonb')
    op.alter_column('organisation', 'organisationType', type_=JSONB(), postgresql_using='"organisationType"::jsonb')
    op.alter_column('source', 'distribution', type_=JSONB(), postgresql_using='"distribution"::jsonb')


def downgrade():
    op.alter_column('organisation', 'location', type_=JSON(), postgresql_using='"location"::json')
    op.alter_column('organisation', 'orgIDs', type_=JSON(), postgresql_using='"orgIDs"::json')
    op.alter_column('organisation', 'alternateName', type_=JSON(), postgresql_using='"alternateName"::json')
    op.alter_column('organisation', 'organisationType', type_=JSON(), postgresql_using='"organisationType"::json')
    op.alter_column('source', 'distribution', type_=JSON(), postgresql_using='"distribution"::json')
