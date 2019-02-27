import pandas as pd
from functoolz import pipeable
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType, StringType, DateType

DTYPES_TO_SPARK_TYPES = {'O':StringType,
                         'i':IntegerType,
                         'f':FloatType,
                         'M':DateType}


def get_spark_types(df, keys=[]):
    sql_type = lambda name, dtype: StructField(name, 
                                               DTYPES_TO_SPARK_TYPES[dtype.kind](),
                                               False if name in keys else True)
    cols_and_dtypes = lambda df: zip(df.columns, df.dtypes)
    return StructType([sql_type(col, dtype) for col, dtype in cols_and_dtypes(df)])



@pipeable
def to_pandas(rows):
    names = list(rows[0].asDict().keys()) # pyspark.Row keys perserve order
    return pd.DataFrame.from_dict([r.asDict() for r in rows])[names] # reorder to original order


col_startswith = pipeable(lambda prefix, cols: [c for c in cols if c.startswith(prefix)])
col_endswith =   pipeable(lambda suffix, cols: [c for c in cols if c.endswith(suffix)])
col_contains =   pipeable(lambda substr, cols: [c for c in cols if substr in c])

def col_selector(col_names, from_=None, to=None, inclusive=True):
    from_idx, to_idx = 0, len(col_names)
    if from_:
        from_idx = col_names.index(from_) if inclusive else col_names.index(from_) + 1
    if to:
        to_idx = col_names.index(to) if not inclusive else col_names.index(to) + 1
    return  col_names[from_idx:to_idx]

def get_column_list(col_names, from_=None, to=None, inclusive=True):
    selected_cols = col_selector(col_names, from_=from_, to=to, inclusive=inclusive)
    return  [c for c in col_names if c in selected_cols]

@pipeable
def cols_from(col, col_names, inclusive=True):
    return  get_column_list(col_names, from_=col, inclusive=inclusive)
    
    
@pipeable
def cols_to(col, col_names, inclusive=True):
    return  get_column_list(col_names, to=col, inclusive=inclusive)
    
    
@pipeable
def cols_between(col1, col2, col_names, inclusive=True):
    return get_column_list(col_names, from_=col1, to=col2, inclusive=inclusive)

@pipeable
def all_but(cols_to_exclude, col_names):
    return [c for c in col_names if c not in cols_to_exclude]
