"""Add scraping table

Revision ID: 2c338e0e9ed7
Revises: c0cec5154a74
Create Date: 2019-10-18 11:19:24.304015

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '2c338e0e9ed7'
down_revision = 'c0cec5154a74'
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table('scrape',
    sa.Column('id', sa.String(), nullable=False),
    sa.Column('spider', sa.String(), nullable=True),
    sa.Column('stats', sa.String(), nullable=True),
    sa.Column('finish_reason', sa.String(), nullable=True),
    sa.Column('errors', sa.Integer(), nullable=True),
    sa.Column('start_time', sa.DateTime(), nullable=True),
    sa.Column('finish_time', sa.DateTime(), nullable=True),
    sa.PrimaryKeyConstraint('id')
    )
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_table('scrape')
    # ### end Alembic commands ###
