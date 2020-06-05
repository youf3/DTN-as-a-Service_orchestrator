"""added interface

Revision ID: 6ff85201f6b7
Revises: 15df1cfe4d0f
Create Date: 2020-06-05 17:51:20.220191

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '6ff85201f6b7'
down_revision = '15df1cfe4d0f'
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    with op.batch_alter_table('DTN', schema=None) as batch_op:
        batch_op.add_column(sa.Column('interface', sa.String(length=15), nullable=True))

    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    with op.batch_alter_table('DTN', schema=None) as batch_op:
        batch_op.drop_column('interface')

    # ### end Alembic commands ###
