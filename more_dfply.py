from collections import defaultdict
from dfply import make_symbolic, pipe, symbolic_evaluation, Intention
import pandas as pd
import numpy as np
import re
from string import punctuation, whitespace
from functoolz import pipeable

STARTS_WITH_DIGITS_REGEX = re.compile(r'\d+')
PUNC_REGEX = re.compile('[{0}]'.format(re.escape(punctuation.replace('_', ''))))
WS_REGEX = re.compile('[{0}]'.format(re.escape(whitespace)))

def fix_name(name:str) -> str:
    """ Makes a column name a proper identifier.
    
    This function will
    
    1. Strip leading/trailing whitspace
    2. Remove all puncuation except _
    3. Add a _ to the start of any string that start with a digit
    """
    strip = pipeable(lambda s: s.strip())
    remove_punc = pipeable(lambda s: PUNC_REGEX.sub('',s))
    fix_starting_digit = pipeable(lambda s: '_' + s if STARTS_WITH_DIGITS_REGEX.match(s) else s)
    replace_whitespace = pipeable(lambda s: WS_REGEX.sub('_', s))
    if name.isidentifier():
        return name
    else:
        return name >> strip >> remove_punc >> fix_starting_digit >> replace_whitespace

def test_fix_name() -> None:
    good_name = 'good_name'
    bad_name = " 1bad_name !1!\n"
    assert fix_name(good_name).isidentifier()
    assert fix_name(bad_name).isidentifier()
test_fix_name()
    

@make_symbolic
def fix_names(df):
    """ Creates a dict of new_name:old_name pairs.
    
    Any name that is not an identifier (using the new .isidentifier predicate) 
    The new names have all punctuation removed, outer whitespace removed,
    and whitespace replaced with _.
    """
    return {fix_name(col):col for col in df.columns}

def test_fix_names():
    good_name = 'good_name'
    bad_name = " 1bad_name!1!\n"
    df = pd.DataFrame({good_name: [1,2,3],
                       bad_name:[1,2,3]})
    assert all(k.isidentifier() for k in fix_names(df))
test_fix_names()


@make_symbolic
def extract(col, pattern):
    if re.compile(pattern).groups != 1:
        raise ValueError('extract requires a pattern with exactly 1 group')
    return col.str.extract(pattern, expand=False)


@make_symbolic
def to_datetime(series, infer_datetime_format=True):
    return pd.to_datetime(series, infer_datetime_format=infer_datetime_format)


@make_symbolic
def recode(col, d, default=None):
    if default is not None:
        new_d = defaultdict(lambda: default)
        new_d.update(d)
        d = new_d
    return col.map(d)


@pipe
@symbolic_evaluation(eval_as_selector=[1])
def set_index(df, col, drop = True):
    return df.set_index(col, drop = drop)


@pipe
@symbolic_evaluation
def row_index_slice(df, *args):
    assert len(args) in (1,2), "loc requires 1-2 arguments"
    if len(args) == 1:
        return df.loc[args[0]]
    else:
        return df.loc[args[0]:args[1]]
    
    
@make_symbolic
def maybe_tile(n, col):
    if not hasattr(col, '__len__') or len(col) < n:
        return pd.Series(np.tile(col, n)[:n])
    else:
        return pd.Series(col[:n])
    
    
def ifelse(cond, then, else_):
    if any(isinstance(o, Intention) for o in (cond, then, else_)):
        def outfunc(df): 
            cond_out = cond.evaluate(df) if isinstance(cond, Intention) else cond
            n = len(cond_out)
            if cond_out.all():
                then_out = then.evaluate(df) if isinstance(then, Intention) else then
                return maybe_tile(n, then_out)
            if not cond_out.any():
                else_out = else_.evaluate(df) if isinstance(else_, Intention) else else_
                return maybe_tile(n, else_out)
            else:
                then_out = then.evaluate(df) if isinstance(then, Intention) else then
                else_out = else_.evaluate(df) if isinstance(else_, Intention) else else_
                return np.where(cond_out, maybe_tile(n, then_out), maybe_tile(n, else_out))
        return Intention(outfunc)
    else:
        n = len(cond)
        return np.where(cond, maybe_tile(n, then), maybe_tile(n, else_))
    
