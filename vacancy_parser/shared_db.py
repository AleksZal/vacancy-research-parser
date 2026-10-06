"""Credential-safe connection and read-only inspection of the shared database."""
from pathlib import Path

import psycopg
from psycopg.conninfo import conninfo_to_dict
from psycopg.rows import dict_row
from dotenv import dotenv_values

from .paths import PROJECT_ROOT

TABLES = (
    'vacancy_source', 'employer_profile', 'vacancy', 'vacancy_hh',
    'vacancy_discovery', 'vacancy_snapshot', 'classifier_run',
    'vacancy_classification', 'employer_classification',
    'classification_evidence', 'employer_company_match',
    'vacancy_company_match', 'manual_review', 'company',
)


def connect_shared(env_path=None):
    values = dotenv_values(env_path or PROJECT_ROOT / '.env')
    url = values.get('DATABASE_URL')
    if not url:
        raise ValueError('DATABASE_URL is missing in the local .env')
    if url.startswith('jdbc:postgresql:'):
        url = url.removeprefix('jdbc:')
    options = conninfo_to_dict(url)
    if values.get('USER'):
        options['user'] = values['USER']
    if values.get('PASSWORD'):
        options['password'] = values['PASSWORD']
    options.update(connect_timeout=20, application_name='hh_research_import', sslmode='require')
    return psycopg.connect(**options, autocommit=True, row_factory=dict_row)


def inspect_schema(conn):
    with conn.transaction():
        conn.execute('SET TRANSACTION READ ONLY')
        columns = conn.execute('''
            SELECT table_schema, table_name, column_name, data_type, udt_name,
                   is_nullable, column_default, ordinal_position
            FROM information_schema.columns
            WHERE table_name = ANY(%s)
            ORDER BY table_schema, table_name, ordinal_position
        ''', (list(TABLES),)).fetchall()
        constraints = conn.execute('''
            SELECT n.nspname AS table_schema, t.relname AS table_name,
                   c.conname AS name, c.contype AS kind,
                   pg_get_constraintdef(c.oid) AS definition
            FROM pg_constraint c
            JOIN pg_class t ON t.oid = c.conrelid
            JOIN pg_namespace n ON n.oid = t.relnamespace
            WHERE t.relname = ANY(%s)
            ORDER BY n.nspname, t.relname, c.conname
        ''', (list(TABLES),)).fetchall()
        indexes = conn.execute('''
            SELECT schemaname, tablename, indexname, indexdef
            FROM pg_indexes WHERE tablename = ANY(%s)
            ORDER BY schemaname, tablename, indexname
        ''', (list(TABLES),)).fetchall()
        triggers = conn.execute('''
            SELECT n.nspname AS table_schema, t.relname AS table_name,
                   g.tgname AS name, pg_get_triggerdef(g.oid) AS definition
            FROM pg_trigger g JOIN pg_class t ON t.oid=g.tgrelid
            JOIN pg_namespace n ON n.oid=t.relnamespace
            WHERE NOT g.tgisinternal AND t.relname=ANY(%s)
        ''', (list(TABLES),)).fetchall()
        enums = conn.execute('''
            SELECT n.nspname AS schema, t.typname AS type, e.enumlabel AS label
            FROM pg_type t JOIN pg_enum e ON e.enumtypid=t.oid
            JOIN pg_namespace n ON n.oid=t.typnamespace
            ORDER BY n.nspname,t.typname,e.enumsortorder
        ''').fetchall()
        return dict(columns=columns, constraints=constraints, indexes=indexes,
                    triggers=triggers, enums=enums)


def safe_error(exc):
    """Do not print a DSN, network address, credentials or failed row values."""
    result = {'error_type': type(exc).__name__}
    if isinstance(exc, psycopg.Error):
        result['sqlstate'] = exc.sqlstate
        for name in ('table_name', 'column_name', 'constraint_name'):
            value = getattr(exc.diag, name, None)
            if value:
                result[name] = value
    return result
