"""table deps save and restore

Revision ID: e7b0c0fba2e3
Revises: bf30b3dbebb4
Create Date: 2020-09-17 14:12:32.436889

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'e7b0c0fba2e3'
down_revision = 'bf30b3dbebb4'
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        'table_dependencies',
        sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
        sa.Column('view_schema', sa.Text(), nullable=False),
        sa.Column('view_name', sa.Text(), nullable=False),
        sa.Column('ddl_to_run', sa.Text(), nullable=False),
        sa.PrimaryKeyConstraint('id'),
        schema='dataflow',
    )

    connection = op.get_bind()
    connection.execute(
        """
        create or replace function dataflow.save_and_drop_dependencies(p_view_schema varchar, p_view_name varchar) returns void as
        $$
        declare
          v_curr record;
        begin
        for v_curr in
        (
          select obj_schema, obj_name, obj_type from
          (
            with recursive recursive_deps(obj_schema, obj_name, obj_type, depth) as
            (
                select p_view_schema, p_view_name, null::varchar, 0
                union
                select dep_schema::varchar, dep_name::varchar, dep_type::varchar, recursive_deps.depth + 1 from
                (
                    select
                        ref_nsp.nspname ref_schema,
                        ref_cl.relname ref_name,
                        rwr_cl.relkind dep_type,
                        rwr_nsp.nspname dep_schema,
                        rwr_cl.relname dep_name
                    from pg_depend dep
                    join pg_class ref_cl on dep.refobjid = ref_cl.oid
                    join pg_namespace ref_nsp on ref_cl.relnamespace = ref_nsp.oid
                    join pg_rewrite rwr on dep.objid = rwr.oid
                    join pg_class rwr_cl on rwr.ev_class = rwr_cl.oid
                    join pg_namespace rwr_nsp on rwr_cl.relnamespace = rwr_nsp.oid
                    where dep.deptype = 'n'
                    and dep.classid = 'pg_rewrite'::regclass
                ) deps
                join recursive_deps on deps.ref_schema = recursive_deps.obj_schema and deps.ref_name = recursive_deps.obj_name
                where (deps.ref_schema != deps.dep_schema or deps.ref_name != deps.dep_name)
            )
            select obj_schema, obj_name, obj_type, depth
            from recursive_deps
            where depth > 0
          ) t
          group by obj_schema, obj_name, obj_type
          order by max(depth) desc
        ) loop
          insert into dataflow.table_dependencies(view_schema, view_name, ddl_to_run)
            select p_view_schema, p_view_name, 'COMMENT ON ' ||
            case
                when c.relkind = 'v' then 'VIEW'
                when c.relkind = 'm' then 'MATERIALIZED VIEW'
                else ''
            end
            || ' ' || n.nspname || '.' || c.relname || ' IS ''' || replace(d.description, '''', '''''') || ''';'
            from pg_class c
            join pg_namespace n on n.oid = c.relnamespace
            join pg_description d on d.objoid = c.oid and d.objsubid = 0
            where n.nspname = v_curr.obj_schema and c.relname = v_curr.obj_name and d.description is not null;

          insert into dataflow.table_dependencies(view_schema, view_name, ddl_to_run)
            select p_view_schema, p_view_name, 'COMMENT ON COLUMN ' || n.nspname || '.' || c.relname || '.' || a.attname || ' IS ''' || replace(d.description, '''', '''''') || ''';'
            from pg_class c
            join pg_attribute a on c.oid = a.attrelid
            join pg_namespace n on n.oid = c.relnamespace
            join pg_description d on d.objoid = c.oid and d.objsubid = a.attnum
            where n.nspname = v_curr.obj_schema and c.relname = v_curr.obj_name and d.description is not null;

          insert into dataflow.table_dependencies(view_schema, view_name, ddl_to_run)
            select p_view_schema, p_view_name, 'GRANT ' || privilege_type || ' ON ' || table_schema || '.' || table_name || ' TO ' || grantee
            from information_schema.role_table_grants
            where table_schema = v_curr.obj_schema and table_name = v_curr.obj_name;

          if v_curr.obj_type = 'v' then
            insert into dataflow.table_dependencies(view_schema, view_name, ddl_to_run)
                    select p_view_schema, p_view_name, 'CREATE VIEW ' || v_curr.obj_schema || '.' || v_curr.obj_name || ' AS ' || view_definition
                    from information_schema.views
                    where table_schema = v_curr.obj_schema and table_name = v_curr.obj_name;
          elsif v_curr.obj_type = 'm' then
            insert into dataflow.table_dependencies(view_schema, view_name, ddl_to_run)
                    select p_view_schema, p_view_name, 'CREATE MATERIALIZED VIEW ' || v_curr.obj_schema || '.' || v_curr.obj_name || ' AS ' || definition
                    from pg_matviews
                    where schemaname = v_curr.obj_schema and matviewname = v_curr.obj_name;
          end if;

          execute 'DROP ' ||
          case
            when v_curr.obj_type = 'v' then 'VIEW'
            when v_curr.obj_type = 'm' then 'MATERIALIZED VIEW'
          end
          || ' ' || v_curr.obj_schema || '.' || v_curr.obj_name;

        end loop;
        end;
        $$
        LANGUAGE plpgsql;

        create or replace function dataflow.restore_dependencies(p_view_schema varchar, p_view_name varchar) returns void as
        $$
        declare
          v_curr record;
        begin
        for v_curr in
        (
          select ddl_to_run
          from dataflow.table_dependencies
          where view_schema = p_view_schema and view_name = p_view_name
          order by id desc
        ) loop
          execute v_curr.ddl_to_run;
        end loop;
        delete from dataflow.table_dependencies
        where view_schema = p_view_schema and view_name = p_view_name;
        end;
        $$
        LANGUAGE plpgsql;
    """
    )


def downgrade():
    op.drop_table('table_dependencies', schema='dataflow')
    connection = op.get_bind()
    connection.execute(
        """
        DROP FUNCTION dataflow.save_and_drop_dependencies, dataflow.restore_dependencies;
    """
    )
