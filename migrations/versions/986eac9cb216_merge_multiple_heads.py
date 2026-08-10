"""merge multiple heads

Revision ID: 986eac9cb216
Revises: 4276764f5a36, add_inheritance_tables
Create Date: 2025-08-17 16:21:13.394739

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '986eac9cb216'
down_revision = ('4276764f5a36', 'add_inheritance_tables')
branch_labels = None
depends_on = None


def upgrade():
    pass


def downgrade():
    pass
