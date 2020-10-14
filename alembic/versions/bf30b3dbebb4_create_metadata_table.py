"""create metadata table

Revision ID: bf30b3dbebb4
Revises:
Create Date: 2020-08-18 16:07:58.707634

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "bf30b3dbebb4"
down_revision = None
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "metadata",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("table_schema", sa.Text(), nullable=False),
        sa.Column("table_name", sa.Text(), nullable=False),
        sa.Column("source_data_modified_utc", sa.DateTime(), nullable=True),
        sa.Column("dataflow_swapped_tables_utc", sa.DateTime(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        schema="dataflow",
    )


def downgrade():
    op.drop_table("metadata", schema="dataflow")
