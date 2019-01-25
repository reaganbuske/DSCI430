from functoolz import pipeable

result_dict = lambda r: {n:v for n,v in zip(r.keys(), r)}
result_dicts = pipeable(lambda rs: [result_dict(r) for r in rs])
