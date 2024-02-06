"""utils/db/connection.py

Author: neo154
Version: 0.1.1
Date Modified: 2024-02-01

Simple methods for connections creation of the DB tables, and populating these DB tables
"""

import re
from datetime import date
from logging import Logger
from typing import Dict, Generator, List, Literal, Union

import numpy as np
import pandas as pd
from sqlalchemy import (URL, Column, ColumnCollection, Connection, Engine,
                        Enum, Inspector, MetaData, Select, String, Text,
                        create_engine, insert, select, text, update)
from sqlalchemy.dialects import postgresql
from sqlalchemy.exc import DataError, OperationalError
from sqlalchemy.orm import Session
from sqlalchemy.schema import CreateTable

from afk import StorageLocation, export_df
from afk.db.models import DeclarativeBaseTable, check_datatypes

_SupportedDialects = ['mysql', 'postgresql']
_SupportedDialectsType = Literal['mysql', 'postgresql']
_SQLQueryReturns = Union[pd.DataFrame, Generator[pd.DataFrame, None, None]]

_DEFAULT_DB_NAME = "SEC_DATA"

_POSTGRESQL_ENUM_QUERY = 'SELECT n.nspname as enum_schema, t.typname as enum_name, e.enumlabel as '\
    + 'enum_value '\
    + 'FROM pg_type t '\
    + 'JOIN pg_enum e ON t.oid = e.enumtypid '\
    + 'JOIN pg_catalog.pg_namespace n ON n.oid = t.typnamespace'


def get_engine(dialect: _SupportedDialectsType, username: str, passwrd: str, host: str,
        driver: str=None, port: int=None, database: str=None) -> Engine:
    """
    Gets a database engine for a DB connection from given information

    :param dialect: Type of database being used like mysql, postgresql, etc.
    :param username: String of the user to login to the DB
    :param passwrd: Sring of the password to login as user to the DB
    :param host: String host for DB connection, can be hostname or IP
    :param driver: String name of the driver to use for connection
    :param port: Integer of the port that hosts DB service
    :param database: String name of the database instance on that host's service
    :returns: Engine object that can be used to connect to DB
    """
    dialect_str = dialect
    if dialect not in _SupportedDialects:
        raise ValueError(f"Dialect provided: {_SupportedDialects} isn't recognized for secdata")
    if driver is not None:
        dialect_str += f'+{driver}'
    if database is None:
        database = _DEFAULT_DB_NAME
    return create_engine( URL.create(
        dialect_str,
        username=username,
        password=passwrd,
        host=host,
        database=database,
        port=port
    ))

def confirm_db_exists(dialect: _SupportedDialectsType, username: str, passwrd: str, host: str,
        driver: str=None, port: int=None, database: str=None) -> bool:
    """
    Confirms if DB exists and is accessible on a host, requires same engine requirements

    :param dialect: Type of database being used like mysql, postgresql, etc.
    :param username: String of the user to login to the DB
    :param passwrd: Sring of the password to login as user to the DB
    :param host: String host for DB connection, can be hostname or IP
    :param driver: String name of the driver to use for connection
    :param port: Integer of the port that hosts DB service
    :param database: String name of the database instance on that host's service
    :returns: Boolean if database is present or not
    """
    dialect_str = dialect
    if dialect not in _SupportedDialects:
        raise ValueError(f"Dialect provided: {_SupportedDialects} isn't recognized for secdata")
    if driver is not None:
        dialect_str += f'+{driver}'
    if database is None:
        database = _DEFAULT_DB_NAME
    if dialect=='postgresql':
        base_url = URL.create(dialect_str, username=username, password=passwrd, host=host,
            port=port, database='postgres')
    else:
        base_url = URL.create(dialect_str, username=username, password=passwrd, host=host,
            port=port)
    tmp_engine1 = create_engine(base_url)
    try:
        tmp_conn = tmp_engine1.connect()
        tmp_conn.close()
    except OperationalError as op_err:
        raise ValueError("Can't connect to host for db check, check host status or creds") \
            from op_err
    tmp_engine2 = create_engine(URL.create(
        dialect_str,
        username=username,
        password=passwrd,
        host=host,
        database=database,
        port=port
    ))
    try:
        tmp_conn = tmp_engine2.connect()
        tmp_conn.close()
        return True
    except OperationalError:
        return False

def confirm_table_exists(binding: Union[Engine, Connection], table: DeclarativeBaseTable) -> bool:
    """
    Confirms if a table in the database is present

    :param binding: Engine or Connection object for database
    :param table: ORM Table
    :returns: Boolean returning True if table is present
    """
    tb_name = table.__tablename__
    if isinstance(binding, Connection):
        tmp_inspector = Inspector(binding)
        return tmp_inspector.has_table(table_name=tb_name)
    with binding.connect() as tmp_conn:
        tmp_inspector = Inspector(tmp_conn)
        ret_value = tmp_inspector.has_table(table_name=tb_name)
    return ret_value

def create_database(dialect: _SupportedDialectsType, username: str, passwrd: str, host: str,
        logger_ref: Logger, driver: str=None, port: int=None, database: str=None) -> None:
    """
    Creating Database for a given name, not supported by postgresql because of it's DB creation
    command isn't supported in transaction mode

    :param dialect: Type of database being used like mysql, postgresql, etc.
    :param username: String of the user to login to the DB
    :param passwrd: Sring of the password to login as user to the DB
    :param host: String host for DB connection, can be hostname or IP
    :param logger_ref: Logger for logging creation of database
    :param driver: String name of the driver to use for connection
    :param port: Integer of the port that hosts DB service
    :param database: String name of the database instance on that host's service
    :returns: Boolean if database is present or not
    """
    if dialect not in _SupportedDialects:
        raise ValueError(f"Dialect provided: {_SupportedDialects} isn't recognized for secdata")
    if dialect=='postgresql':
        raise ValueError("This isn't supported in postgres due to transaction blocks")
    if database is None:
        database = _DEFAULT_DB_NAME
    logger_ref.info("Creating for %s database with name %s if not exists", dialect, database)
    dialect_str = dialect
    if driver is not None:
        dialect_str += f'+{driver}'
    engine_url = URL.create(dialect_str, username=username, password=passwrd, host=host,
        port=port)
    query = f"CREATE DATABASE IF NOT EXISTS {database};"
    with create_engine(engine_url, isolation_level='AUTOCOMMIT').connect() as connection:
        connection.execute(text(query))
        connection.commit()

def _get_postgres_enums(bind: Connection) -> pd.DataFrame:
    """
    Fetches postgres enumeration types that have been established in the DB

    :param bind: DB Connection for a postgres DB
    :returns: DataFrame of all enumerations including their names and values
    """
    result_curs = bind.execute(text(_POSTGRESQL_ENUM_QUERY))
    return pd.DataFrame(result_curs.all())

def _handle_postgres_col_creation(bind: Connection, col: Column, logger_ref: Logger) -> None:
    """
    Handles the creation or replacment of a given enum

    :param bind: DB Connection to postgres DB
    :param col: Column object that is being evaluated to create any required subtypes for postgres
    :param logger_ref: Logger object
    :return: None
    """
    col_type = col.type
    if isinstance(col_type, Enum):
        logger_ref.debug("Identified an enum in postgres")
        current_enums_df = _get_postgres_enums(bind)
        if current_enums_df.index.size<=0 \
                or not col_type.name in current_enums_df['enum_name'].tolist():
            logger_ref.info("Creating enum type for table")
            col_type.create(bind)
            return
        logger_ref.debug("Identified prexisting ENUM in postgres DB")
        enum_values = current_enums_df[current_enums_df['enum_name']==col_type.name]['enum_value']\
            .tolist()
        missing_enums = list(set(col_type.enums).difference(enum_values))
        if missing_enums:
            raise ValueError(f"Enum declared and is missing values in DB: {missing_enums}")

def create_tables(db_tables: Union[DeclarativeBaseTable, List[DeclarativeBaseTable]],
        db_engine: Engine, logger_ref: Logger) -> None:
    """
    Generates tables from a single or list of given tables

    :param db_tables: Single or List of tables to create in a given DB
    :param db_engine: Engine to be able to connect to the database
    :param logger_ref: Logger object
    :returns: None
    """
    if not isinstance(db_tables, list):
        db_tables = [db_tables]
    logger_ref.info("Getting set to confirm tables exist")
    if db_engine.name!='postgresql':
        tmp_metadata = MetaData()
        tmp_metadata.create_all(db_engine, [entry.__table__ for entry in db_tables], True)
        return
    logger_ref.debug("Postgres so having to use manual CreateTable commands")
    with db_engine.connect() as db_conn:
        for entry in db_tables:
            # Need to go through table and confirm that it's enums get created if they exist
            if not confirm_table_exists(db_conn, entry):
                for col in entry.__table__.columns:
                    _handle_postgres_col_creation(db_conn, col, logger_ref)
                create_text = str(CreateTable(entry.__table__)\
                    .compile(dialect=postgresql.dialect()))
                db_conn.execute(text(re.sub(r"TEXT\s*\([0-9]+\)", 'TEXT', create_text)))
                db_conn.commit()

def _check_col_default(col: Column) -> bool:
    """
    Checks if column has default pop values, helps identify if we can ignore or not require
    values in those given columns

    :param col: Column to identify state of autopopulation
    :returns: Boolean True if there is a default value or does autoincrement
    """
    return col.default or col.autoincrement==True # pylint: disable=singleton-comparison

def df_bulk_insert_to_db(p_df: pd.DataFrame, table: DeclarativeBaseTable,
        df_2_db_mapper: Dict[str, str], db_engine: Engine, fail_extract_dir: StorageLocation,
        logger_ref: Logger, chunksize: int=100000, export_datafile_on_fail: bool=False) -> None:
    """
    Takes a dataframe to be loaded in a table and executes bulk imports from the dataframe into
    a DB for a given table

    Bulk upload functions already do chunking for inserts, but chunksize to keep memory manageable
    for creating upload copies of the DF after conversions too

    :param p_df: Dataframe containing data to bulk insert into the DB
    :param table: Table to insert data in the DB
    :param df_2_db_mapper: Dictionary of DataFrame to DB names
    :param db_engine: Engine to be able to connect to the database
    :param fail_extract_dir: StorageLocation, dir-like, to store any failed uploads
    :param logger_ref: Logger object
    :param chunksize: Integer of number of records to transform and prep for bulk insertion
    :param export_datafile_on_fail: Boolean indicating to export entire datafile if it fails
    :returns: None
    """
    upload_df = p_df.copy(deep=True)
    if fail_extract_dir.exists() and not fail_extract_dir.is_dir():
        raise ValueError("fail_extract_dir needs to be a directory area for rejected record info")
    _table_name = table.__tablename__
    if not confirm_table_exists(db_engine, table):
        raise ValueError(f"Table {_table_name} isn't found in DB or connect issue to host")
    date_str = date.today().strftime("%Y_%m_%d")
    rejected_datafile_name: StorageLocation = fail_extract_dir\
        .join_loc(f'rejected_insert_records_{_table_name}_{date_str}.csv')
    rejected_paramfile_name: StorageLocation = fail_extract_dir\
        .join_loc(f'rejected_insert_params_{_table_name}_{date_str}.csv')
    logger_ref.info("Checking data to be inserted into %s", _table_name)
    check_datatypes(upload_df, df_2_db_mapper, table)
    for datetime_col in upload_df.columns[upload_df.dtypes=='datetime64[ns, UTC]']:
        upload_df[datetime_col] = upload_df[datetime_col].dt.tz_localize(None)
    # Sanity check on pks for table
    db_df = upload_df[list(df_2_db_mapper.keys())].rename(columns=df_2_db_mapper)
    table_cols: ColumnCollection = table.__table__.columns
    tb_col: Column
    for tb_col in table_cols:
        if not _check_col_default(tb_col):
            if not tb_col.name in db_df:
                raise ValueError(f"Missing Colum that isn't auto-populated by DB: {tb_col.name}")
            if isinstance(tb_col.type, (Enum, Text, String)):
                db_df[tb_col.name] = db_df[tb_col.name].fillna(np.nan).replace([np.nan], [None])
    record_start = 0
    df_len = db_df.index.size
    logger_ref.info("Starting upload of records")
    with db_engine.connect() as connection:
        with Session(bind=connection) as session:
            try:
                while record_start < df_len:
                    end = record_start + chunksize
                    logger_ref.debug("Inserting records %d-%d", record_start, end)
                    session.execute(insert(table), db_df[record_start: end]\
                        .to_dict(orient='records'))
                    record_start += chunksize
                logger_ref.info("Commiting to database")
                session.commit()
            except DataError as data_error:
                logger_ref.error("Issue detected in database inserts, rolling back and exporting")
                if export_datafile_on_fail:
                    logger_ref.info("Exporting full datafile for rejected records")
                    export_df(p_df, rejected_datafile_name, logger_ref=logger_ref)
                with rejected_paramfile_name.open('w', encoding='utf-8') as rjt_param_open:
                    _ = rjt_param_open.write(str(data_error.statement))
                    _ = rjt_param_open.write('\n')
                    _ = rjt_param_open.write(str(data_error.params))
                session.rollback()
                raise data_error

def df_bulk_update_to_db(update_df: pd.DataFrame, table: DeclarativeBaseTable,
        df_2_db_mapper: Dict[str, str], db_engine: Engine, fail_extract_dir: StorageLocation,
        logger_ref: Logger, chunksize: int=100000, export_datafile_on_fail: bool=False) -> None:
    """
    Takes a given Dataframe and does bulk update to a database, will confirm that PK fields are
    present and do normal datachecking on columns that are present

    :param p_df: Dataframe containing data to bulk insert into the DB
    :param table: Table to insert data in the DB
    :param df_2_db_mapper: Dictionary of DataFrame to DB names
    :param db_engine: Engine to be able to connect to the database
    :param fail_extract_dir: StorageLocation, dir-like, to store any failed uploads
    :param logger_ref: Logger object
    :param chunksize: Integer of number of records to transform and prep for bulk insertion
    :param export_datafile_on_fail: Boolean indicating to export entire datafile if it fails
    :returns: None
    """
    upload_df = update_df.copy(deep=True)
    _table_name = table.__tablename__
    if fail_extract_dir.exists() and not fail_extract_dir.is_dir():
        raise ValueError("fail_extract_dir needs to be a directory area for rejected record info")
    if not confirm_table_exists(db_engine, table):
        raise ValueError(f"Table {_table_name} isn't found in DB or connect issue to host")
    update_mapper = {}
    for df_col, db_col in df_2_db_mapper.items():
        if df_col in upload_df:
            update_mapper[df_col] = db_col
    for pk_col in table.__table__.primary_key:
        if pk_col.name not in update_mapper.values():
            raise ValueError(f"Primary column {pk_col.name} is missing in the given update_df")
    date_str = date.today().strftime("%Y_%m_%d")
    rejected_datafile_name: StorageLocation = fail_extract_dir\
        .join_loc(f'rejected_update_records_{_table_name}_{date_str}.csv')
    rejected_paramfile_name: StorageLocation = fail_extract_dir\
        .join_loc(f'rejected_update_params_{_table_name}_{date_str}.csv')
    logger_ref.info("Checking data to be updated into %s", _table_name)
    check_datatypes(upload_df, update_mapper, table)
    dt_col_names = upload_df.columns[upload_df.dtypes=='datetime64[ns, UTC]']
    for datetime_col in dt_col_names:
        upload_df[datetime_col] = upload_df[datetime_col].dt.tz_localize(None)
    db_df = upload_df[list(df_2_db_mapper.keys())].rename(columns=df_2_db_mapper)
    for db_col in db_df.columns:
        if isinstance(table.__table__.c[db_col].type, Enum):
            db_df[db_col] = db_df[db_col].fillna(np.nan).replace([np.nan], [None])
    record_start = 0
    df_len = db_df.index.size
    # Need something we need to replace and fill in the NA values with standard None
    logger_ref.info("Starting upload of records")
    with db_engine.connect() as connection:
        with Session(bind=connection) as session:
            try:
                while record_start < df_len:
                    end = record_start + chunksize
                    logger_ref.debug("Inserting records %d-%d", record_start, end)
                    session.execute(update(table), db_df[record_start: end]\
                        .to_dict(orient='records'))
                    record_start += chunksize
                logger_ref.info("Commiting to database")
                session.commit()
            except DataError as data_error:
                logger_ref.error("Issue detected in database inserts, rolling back and exporting")
                if export_datafile_on_fail:
                    logger_ref.info("Exporting full datafile for rejected records")
                    export_df(update_df, rejected_datafile_name, logger_ref=logger_ref)
                with rejected_paramfile_name.open('w', encoding='utf-8') as rjt_param_open:
                    _ = rjt_param_open.write(str(data_error.statement))
                    _ = rjt_param_open.write('\n')
                    _ = rjt_param_open.write(str(data_error.params))
                session.rollback()
                raise data_error

def _chunk_result_df(db_conn: Connection, query: Select, chunksize: int,
        db_2_df_mapper: Dict[str, str]=None) -> Generator[pd.DataFrame, None, None]:
    """
    Chunks Cursor results from the dtabase return into a generator

    :param cursor_r: CursorResult object that is used to fetch results from Database
    :param chunksize: Integer of number of records per batch from the DB
    :param db_2_df_mapper: Dictionary containing column renames where applicable
    :yields: DataFrame of results from Database
    """
    with pd.read_sql_query(query, db_conn, chunksize=chunksize) as pd_sql_gen:
        for chunk_df in pd_sql_gen:
            if db_2_df_mapper is not None:
                yield chunk_df.rename(columns=db_2_df_mapper)
            else:
                yield chunk_df

def select_2_df(db_engine: Engine, query: Select, db_2_df_mapper: Dict[str, str]=None,
        chunksize: int=None) -> _SQLQueryReturns:
    """
    Runs a select statement and returns a Dataframe from the Database query

    :param db_engine: Engine object used to execute queries on the Database
    :param query: Select query statement to run on the DB
    :param db_2_df_mapper: Dictionary containing column renames where applicable
    :param chunksize: Integer of number of records per batch from the DB, creates generator
    :returns: DataFrame or a Generator for DataFrames from results from query execution
    """
    if not isinstance(query, Select):
        raise ValueError("Query provided wasn't a select statement")
    with db_engine.connect() as db_conn:
        if chunksize is not None:
            return _chunk_result_df(db_conn, query, chunksize, db_2_df_mapper)
        ret_df = pd.read_sql_query(query, db_conn)
        if db_2_df_mapper is not None:
            ret_df = ret_df.rename(columns=db_2_df_mapper)
        return ret_df

def get_table_df(db_engine: Engine, table: DeclarativeBaseTable, db_2_df_mapper: Dict[str, str],
        chunksize: int=None) -> _SQLQueryReturns:
    """
    Runs a specific query on the database to get the full table's contents of the data

    :param db_engine: Engine object used to execute queries on the Database
    :param query: Select query statement to run on the DB
    :param db_2_df_mapper: Dictionary containing column renames where applicable
    :param chunksize: Integer of number of records per batch from the DB, creates generator
    :returns: DataFrame or a Generator for DataFrames from results from query execution
    """
    return select_2_df(db_engine, select(table), db_2_df_mapper, chunksize)

def get_table_pk_df(db_engine: Engine, table: DeclarativeBaseTable,
        db_2_df_mapper: Dict[str, str], chunksize: int=None) -> _SQLQueryReturns:
    """
    Gets table and the primary key columns only

    :param db_engine: Engine object used to execute queries on the Database
    :param query: Select query statement to run on the DB
    :param db_2_df_mapper: Dictionary containing column renames where applicable
    :param chunksize: Integer of number of records per batch from the DB, creates generator
    :returns: DataFrame or a Generator for DataFrames from results from query execution
    """
    pk_mapper = {}
    pk_cols: ColumnCollection = table.__table__.primary_key.columns
    if len(pk_cols) <= 0:
        raise ValueError("Table provided doesn't have a primary key to select")
    for pk_col in pk_cols:
        if pk_col.name in db_2_df_mapper:
            pk_mapper[pk_col.name] = db_2_df_mapper[pk_col.name]
    return select_2_df(db_engine, select(pk_cols), db_2_df_mapper,
        chunksize)
