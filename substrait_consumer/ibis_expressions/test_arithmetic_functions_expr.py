from substrait_consumer.functional.common import substrait_producer_ibis_test


@substrait_producer_ibis_test("function", "arithmetic")
def test_add_expr(partsupp):
    limited = partsupp.limit(10)
    new_col = (limited.ps_partkey + limited.ps_suppkey).name("ADD_KEY")
    return limited[limited.ps_partkey, limited.ps_suppkey, new_col]


@substrait_producer_ibis_test("function", "arithmetic")
def test_subtract_expr(partsupp):
    limited = partsupp.limit(10)
    new_col = (limited.ps_partkey - limited.ps_suppkey).name("SUBTRACT_KEY")
    return limited[limited.ps_partkey, limited.ps_suppkey, new_col]


@substrait_producer_ibis_test("function", "arithmetic")
def test_multiply_expr(partsupp):
    limited = partsupp.limit(10)
    new_col = (limited.ps_partkey * 10).name("MULTIPLY_KEY")
    return limited[limited.ps_partkey, new_col]


@substrait_producer_ibis_test("function", "arithmetic")
def test_divide_expr(partsupp):
    limited = partsupp.limit(10)
    new_col = (limited.ps_partkey / 10).name("DIVIDE_KEY")
    return limited[limited.ps_partkey, new_col]


@substrait_producer_ibis_test("function", "arithmetic")
def test_modulus_expr(partsupp):
    limited = partsupp.limit(10)
    new_col = (limited.ps_partkey % 10).name("MODULUS_KEY")
    return limited[limited.ps_partkey, new_col]


@substrait_producer_ibis_test("function", "arithmetic")
def test_exp_expr(partsupp):
    limited = partsupp.limit(10)
    new_col = (limited.ps_partkey.exp()).round(2).name("EXP_KEY")
    return limited[limited.ps_partkey, new_col]


@substrait_producer_ibis_test("function", "arithmetic")
def test_power_expr(partsupp):
    limited = partsupp.limit(10)
    new_col = (limited.ps_partkey.pow(2)).name("POWER_KEY")
    return limited[limited.ps_partkey, new_col]


@substrait_producer_ibis_test("function", "arithmetic")
def test_sqrt_expr(partsupp):
    limited = partsupp.limit(10)
    new_col = (limited.ps_partkey.sqrt()).round(2).name("SQRT_KEY")
    return limited[limited.ps_partkey, new_col]


@substrait_producer_ibis_test("function", "arithmetic")
def test_abs_expr(t):
    limited = t.limit(10)
    new_col = limited.a.abs().name("ABS_A")
    return limited[limited.a, new_col]


@substrait_producer_ibis_test("function", "arithmetic")
def test_sign_expr(t):
    limited = t.limit(10)
    new_col = limited.a.sign().name("SIGN_A")
    return limited[limited.a, new_col]


@substrait_producer_ibis_test("function", "arithmetic")
def test_negate_expr(partsupp):
    limited = partsupp.limit(10)
    new_col = (limited.ps_partkey.negate()).name("NEGATE_KEY")
    return limited[limited.ps_partkey, new_col]


@substrait_producer_ibis_test("function", "arithmetic")
def test_sin_expr(partsupp):
    limited = partsupp.limit(10)
    new_col = limited.ps_supplycost.sin().round(2).name("SIN_SUPPLY")
    return limited[new_col]


@substrait_producer_ibis_test("function", "arithmetic")
def test_cos_expr(partsupp):
    limited = partsupp.limit(10)
    new_col = limited.ps_supplycost.cos().round(2).name("COS_SUPPLY")
    return limited[new_col]


@substrait_producer_ibis_test("function", "arithmetic")
def test_tan_expr(partsupp):
    limited = partsupp.limit(10)
    new_col = limited.ps_supplycost.tan().round(2).name("TAN_SUPPLY")
    return limited[new_col]


@substrait_producer_ibis_test("function", "arithmetic")
def test_asin_expr(lineitem):
    limited = lineitem.limit(10)
    new_col = limited.l_tax.asin().round(2).name("ASIN_TAX")
    return limited[new_col]


@substrait_producer_ibis_test("function", "arithmetic")
def test_acos_expr(lineitem):
    limited = lineitem.limit(10)
    new_col = limited.l_tax.acos().round(2).name("ACOS_TAX")
    return limited[new_col]


@substrait_producer_ibis_test("function", "arithmetic")
def test_atan_expr(lineitem):
    limited = lineitem.limit(10)
    new_col = limited.l_tax.atan().round(2).name("ATAN_TAX")
    return limited[new_col]


@substrait_producer_ibis_test("function", "arithmetic")
def test_sum_expr(partsupp):
    stats = [partsupp.ps_supplycost.sum().name("SUM_SUPPLYCOST")]
    return partsupp.aggregate(stats)


@substrait_producer_ibis_test("function", "arithmetic")
def test_min_expr(partsupp):
    stats = [partsupp.ps_supplycost.min().name("MIN_SUPPLYCOST")]
    return partsupp.aggregate(stats)


@substrait_producer_ibis_test("function", "arithmetic")
def test_avg_expr(partsupp):
    stats = [partsupp.ps_supplycost.mean().name("AVG_SUPPLYCOST")]
    return partsupp.aggregate(stats)


@substrait_producer_ibis_test("function", "arithmetic")
def test_max_expr(partsupp):
    stats = [partsupp.ps_supplycost.max().name("MAX_SUPPLYCOST")]
    return partsupp.aggregate(stats)


@substrait_producer_ibis_test("function", "arithmetic")
def test_median_expr(partsupp):
    stats = [
        partsupp.ps_supplycost.cast("float64").approx_median().name("MEDIAN_SUPPLYCOST")
    ]
    return partsupp.aggregate(stats)
