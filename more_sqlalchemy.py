from functoolz import pipeable
result_dict = lambda r: dict(zip(r.keys(), r))
result_dicts = pipeable(lambda rs: list(map(result_dict, rs)))
check_unique = pipeable(lambda df: [(col, df[col].is_unique) for col in df.columns])