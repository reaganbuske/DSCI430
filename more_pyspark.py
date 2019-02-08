import pandas as pd
from functoolz import pipeable

to_pandas = pipeable(lambda rows: pd.DataFrame.from_dict([r.asDict() for r in rows]))