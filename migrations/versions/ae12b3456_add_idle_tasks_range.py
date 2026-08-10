"""add start_from/end_at to idle_tasks

Revision ID: ae12b3456
Revises: 9febedacc5e5
Create Date: 2025-09-13 00:00:00.000000
"""
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = 'ae12b3456'
down_revision = '9febedacc5e5'
branch_labels = None
depends_on = None


def upgrade():
    bind = op.get_bind()
    inspector = sa.inspect(bind)

    # Add new columns with sensible server defaults to avoid locking large tables
    op.add_column('idle_tasks', sa.Column('start_from', sa.Integer(), nullable=False, server_default='0'))
    op.add_column('idle_tasks', sa.Column('end_at', sa.Integer(), nullable=False, server_default='999999'))

    # Clear any existing idle_tasks to avoid partial/invalid rows after schema change
    try:
        op.execute("DELETE FROM idle_tasks")
    except Exception:
        pass

    # Reset idle_status singleton to defaults to ensure fresh state
    try:
        op.execute("UPDATE idle_status SET enabled = true, active = false, current_code = NULL, current_number = NULL, empty_streak = 0 WHERE id = 1")
    except Exception:
        # If idle_status doesn't exist yet, ignore
        pass

    # If an old 'numer' column exists, migrate values into the new range columns and drop it
    cols = [c['name'] for c in inspector.get_columns('idle_tasks')]
    if 'numer' in cols:
        # Set start_from=end_at=numer for existing rows
        op.execute("""
            UPDATE idle_tasks
            SET start_from = numer, end_at = numer
            WHERE numer IS NOT NULL
        """)

        # Now drop the old column
        try:
            op.drop_column('idle_tasks', 'numer')
        except Exception:
            # If drop fails (unexpected), continue - manual cleanup may be required
            pass

    # Create helpful indexes
    try:
        op.create_index('idx_idle_tasks_status', 'idle_tasks', ['status'])
    except Exception:
        pass

    try:
        op.create_index('idx_idle_tasks_kod_start', 'idle_tasks', ['kod_wydzialu', 'start_from'])
    except Exception:
        pass


def downgrade():
    bind = op.get_bind()
    inspector = sa.inspect(bind)

    cols = [c['name'] for c in inspector.get_columns('idle_tasks')]

    # Recreate numer column if it does not exist
    if 'numer' not in cols:
        op.add_column('idle_tasks', sa.Column('numer', sa.Integer(), nullable=True))
        # Populate numer from start_from when range narrow (start==end)
        try:
            op.execute("""
                UPDATE idle_tasks
                SET numer = start_from
                WHERE start_from = end_at
            """)
        except Exception:
            pass

    # Reset idle_status singleton on downgrade as well (best-effort)
    try:
        op.execute("UPDATE idle_status SET enabled = true, active = false, current_code = NULL, current_number = NULL, empty_streak = 0 WHERE id = 1")
    except Exception:
        pass

    # Drop indexes if present
    try:
        op.drop_index('idx_idle_tasks_status', table_name='idle_tasks')
    except Exception:
        pass

    try:
        op.drop_index('idx_idle_tasks_kod_start', table_name='idle_tasks')
    except Exception:
        pass

    # Drop new columns
    try:
        op.drop_column('idle_tasks', 'start_from')
    except Exception:
        pass

    try:
        op.drop_column('idle_tasks', 'end_at')
    except Exception:
        pass
