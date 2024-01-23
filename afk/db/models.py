"""afk/db/models.py

Author: neo154
Version: 0.1.0
Date Modified: 2024-01-10

Module containing some basic helper functions and data type checking for tables
"""

from typing import Any, Dict, List, Tuple

import pandas as pd
from sqlalchemy import (BigInteger, Boolean, Date, DateTime, Double, Enum,
                        Float, Integer, SmallInteger, String, Text)
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.types import TypeEngine


class DeclarativeBaseTable(DeclarativeBase):
    """Declarative Base Table for new tables"""

class DataTypeCheckError(Exception):
    """Datatypes specific errors"""
    def __init__(self, col_name: str, message: str) -> None:
        final_message = f"Detected datatype issue in {col_name}: {message}"
        super().__init__(final_message)

def col_datatype_checking(col_s: pd.Series, col_name: str, sql_type: TypeEngine[Any],
        nullable: bool) -> None:
    """
    Checks the data for all currently supported column types and the data that would be used to
    populate those columns in a database

    :param col_s: Series of any data that is being checked for specific value types DB requirements
    :param col_name: String of column name for helping identify in errors
    :param sql_type: SQLAlchemy Object type for a column in the DB
    :param nullable: Boolean indicating if null values are allowed
    :returns: None
    :raises: DataTypeCheckError for any detected issues
    """
    na_s = col_s.isna()
    if na_s.any() and not nullable:
        raise ValueError(f"{col_name} contains nulls when it cannot have nulls in table")
    all_na = na_s.all()
    infered_type = pd.api.types.infer_dtype(col_s[~na_s])
    if not isinstance(sql_type, (String, Text, Enum, DateTime, Date, Boolean, Float, Double,
            Integer, SmallInteger, BigInteger)):
        raise DataTypeCheckError(col_name, f"Unsupported column check on {sql_type}")
    if isinstance(sql_type, (String, Text, Enum)) and not all_na:
        if not (pd.api.types.is_categorical_dtype(col_s)\
                or infered_type=='string'):
            raise DataTypeCheckError(col_name, "String and NA values only in column")
        if isinstance(sql_type, (String, Text)):
            s_len = sql_type.length
            if col_s[~na_s].apply(len).max()>sql_type.length:
                raise ValueError(f"Some values are longer than {s_len}, not allowed in {col_name}")
        else:
            if (~col_s[~na_s].isin(sql_type.enums)).all():
                raise DataTypeCheckError(col_name, "Some values are not found in the enum")
    if isinstance(sql_type, DateTime) and col_s.dtype!='datetime64[ns, UTC]':
        raise DataTypeCheckError(col_name, "UTC TimeZone information required")
    if isinstance(sql_type, Date) and infered_type!='date':
        raise DataTypeCheckError(col_name, "Date object required in column")
    if isinstance(sql_type, Boolean) and not pd.api.types.is_bool_dtype(col_s):
        raise DataTypeCheckError(col_name, "All Boolean values required in columns")
    if isinstance(sql_type, (Float, Double)) \
            and not pd.api.types.is_float_dtype(col_s):
        raise DataTypeCheckError(col_name, 'Fload data required in column')
    if isinstance(sql_type, (Integer, SmallInteger, BigInteger)) \
            and not pd.api.types.is_integer_dtype(col_s):
        raise DataTypeCheckError(col_name, "All Integer values required in column")

def check_datatypes(p_df: pd.DataFrame, df_2_db_mapper: Dict[str, str],
        table: DeclarativeBaseTable) -> None:
    """
    Checks datatypes for all columns that are to be mapped and loaded into a table,\
    runs a single datatype checking per column

    :param p_df: DataFrame containing data to be loaded into DB
    :param df_2_db_mapper: Dictionary of column names to DB names
    :param table: ORM object that contains all definitions for DB table
    :returns: None
    """
    raw_table_cols = table.__table__.columns
    for key, value in df_2_db_mapper.items():
        sql_col_ref = raw_table_cols[value]
        col_datatype_checking(p_df[key], key, sql_col_ref.type, sql_col_ref.nullable)

def generate_mappers(init_mapper: Dict[str, str], table: DeclarativeBaseTable,
        auto_pop_cols: List[str]=None) -> Tuple[Dict[str, str], Dict[str, str]]:
    """
    For generating mappers for translating pandas dataframes to SQL tables vice-versa,
    does a check also to make sure there aren't conflicting names in one direction

    :param init_mapper: Dictionary of DB 2 DF column name mapping
    :param table: ORM object that contains all definitions for DB table
    :param auto_pop_cols: List of DB names that are autopopulated if it is required to be ignored
    :returns: Tuple of the DB 2 DF and DF 2 DB mappers
    """
    if auto_pop_cols is None:
        auto_pop_cols = []
    missing_cols = list(set(table.__table__.columns.keys()).difference(auto_pop_cols)\
        .difference(init_mapper.keys()))
    if missing_cols:
        raise ValueError(f"Provided mapper is missing columns: {missing_cols}")
    ret_db_2_df = init_mapper
    ret_df_2_db = {}
    for sql_col_name, df_col_name in init_mapper.items():
        if df_col_name in ret_df_2_db:
            raise ValueError(f"Detected duplicate columns for dataframes in mapping {df_col_name}")
        ret_df_2_db[df_col_name] = sql_col_name
    return (ret_db_2_df, ret_df_2_db)
