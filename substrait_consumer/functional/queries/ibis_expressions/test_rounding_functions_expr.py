from substrait_consumer.functional.common import substrait_producer_ibis_test


@substrait_producer_ibis_test("function", "rounding")
def test_ceil_expr(partsupp):
    limited = partsupp.limit(10)
    new_col = limited.ps_supplycost.ceil().name("CEIL_SUPPLYCOST")
    return limited[limited.ps_supplycost, new_col]


@substrait_producer_ibis_test("function", "rounding")
def test_floor_expr(partsupp):
    limited = partsupp.limit(10)
    new_col = limited.ps_supplycost.floor().name("FLOOR_SUPPLYCOST")
    return limited[limited.ps_supplycost, new_col]


@substrait_producer_ibis_test("function", "rounding")
def test_round_expr(lineitem):
    limited = lineitem.limit(10)
    new_col = limited.l_extendedprice.round(digits=1).name("ROUND_EXTENDEDPRICE")
    return limited[limited.l_extendedprice, new_col]
