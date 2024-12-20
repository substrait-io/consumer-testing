from substrait_consumer.functional.common import substrait_producer_ibis_test


@substrait_producer_ibis_test("function", "logarithmic")
def test_ln_expr(partsupp):
    limited = partsupp.limit(10)
    new_col = limited.ps_supplycost.ln().round(digits=2).name("LN_SUPPLY")
    return limited[limited.ps_supplycost, new_col]


@substrait_producer_ibis_test("function", "logarithmic")
def test_log10_expr(partsupp):
    limited = partsupp.limit(10)
    new_col = limited.ps_supplycost.log10().round(digits=2).name("LOG10_SUPPLY")
    return limited[limited.ps_supplycost, new_col]


@substrait_producer_ibis_test("function", "logarithmic")
def test_log2_expr(partsupp):
    limited = partsupp.limit(10)
    new_col = limited.ps_supplycost.log2().round(digits=2).name("LOG2_SUPPLY")
    return limited[limited.ps_supplycost, new_col]


@substrait_producer_ibis_test("function", "logarithmic")
def test_logb_expr(partsupp):
    limited = partsupp.limit(10)
    new_col = limited.ps_supplycost.log(3).round(2).name("LOGB_3_SUPPLY")
    return limited[limited.ps_supplycost, new_col]
