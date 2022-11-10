def add_expr(partsupp, lineitem, t):
    new_col = (partsupp.ps_partkey + partsupp.ps_suppkey).name("ADD_KEY")
    return partsupp[partsupp.ps_partkey, partsupp.ps_suppkey, new_col]


def subtract_expr(partsupp, lineitem, t):
    new_col = (partsupp.ps_partkey - partsupp.ps_suppkey).name("SUBSTRACT_KEY")
    return partsupp[partsupp.ps_partkey, partsupp.ps_suppkey, new_col]


def multiply_expr(partsupp, lineitem, t):
    new_col = (partsupp.ps_partkey * 10).name("MULTIPLY_KEY")
    return partsupp[partsupp.ps_partkey, new_col]


def divide_expr(partsupp, lineitem, t):
    new_col = (partsupp.ps_partkey / 10).name("DIVIDE_KEY")
    return partsupp[partsupp.ps_partkey, new_col]


def modulus_expr(partsupp, lineitem, t):
    new_col = (partsupp.ps_partkey % 10).name("MODULUS_KEY")
    return partsupp[partsupp.ps_partkey, new_col]


def exp_expr(partsupp, lineitem, t):
    new_col = (partsupp.ps_partkey.exp()).name("EXP_KEY")
    return partsupp[partsupp.ps_partkey, new_col]


def power_expr(partsupp, lineitem, t):
    new_col = (partsupp.ps_partkey.pow(2)).name("POWER_KEY")
    return partsupp[partsupp.ps_partkey, new_col]


def sqrt_expr(partsupp, lineitem, t):
    new_col = (partsupp.ps_partkey.sqrt()).name("SQRT_KEY")
    return partsupp[partsupp.ps_partkey, new_col]


def abs_expr(partsupp, lineitem, t):
    new_col = t.a.abs().name("ABS_A")
    return t[t.a, new_col]


def sign_expr(partsupp, lineitem, t):
    new_col = t.a.sign().name("SIGN_A")
    return t[t.a, new_col]


def negate_expr(partsupp, lineitem, t):
    new_col = (partsupp.ps_partkey.negate()).name("NEGATE_KEY")
    return partsupp[partsupp.ps_partkey, new_col]


def sin_expr(partsupp, lineitem, t):
    new_col = partsupp.ps_supplycost.sin().name("SIN_SUPPLY")
    return partsupp[new_col]


def cos_expr(partsupp, lineitem, t):
    new_col = partsupp.ps_supplycost.cos().name("COS_SUPPLY")
    return partsupp[new_col]


def tan_expr(partsupp, lineitem, t):
    new_col = partsupp.ps_supplycost.tan().name("TAN_SUPPLY")
    return partsupp[new_col]


def asin_expr(partsupp, lineitem, t):
    new_col = lineitem.l_tax.asin().name("ASIN_TAX")
    return lineitem[new_col]


def acos_expr(partsupp, lineitem, t):
    new_col = lineitem.l_tax.acos().name("ACOS_TAX")
    return lineitem[new_col]


def atan_expr(partsupp, lineitem, t):
    new_col = lineitem.l_tax.atan().name("ATAN_TAX")
    return lineitem[new_col]


IBIS_SCALAR = {
    "add": add_expr,
    "subtract": subtract_expr,
    "multiply": multiply_expr,
    "divide": divide_expr,
    "modulus": modulus_expr,
    "exp": exp_expr,
    "power": power_expr,
    "sqrt": sqrt_expr,
    "abs": abs_expr,
    "sign": sign_expr,
    "negate": negate_expr,
    "sin": sin_expr,
    "cos": cos_expr,
    "tan": tan_expr,
    "asin": asin_expr,
    "acos": acos_expr,
    "atan": atan_expr,
}


def sum_expr(partsupp, lineitem, t):
    stats = [partsupp.ps_supplycost.sum().name("SUM_SUPPLYCOST")]
    return partsupp.aggregate(stats)


def min_expr(partsupp, lineitem, t):
    stats = [partsupp.ps_supplycost.min().name("MIN_SUPPLYCOST")]
    return partsupp.aggregate(stats)


def avg_expr(partsupp, lineitem, t):
    stats = [partsupp.ps_supplycost.mean().name("AVG_SUPPLYCOST")]
    return partsupp.aggregate(stats)


def max_expr(partsupp, lineitem, t):
    stats = [partsupp.ps_supplycost.max().name("MAX_SUPPLYCOST")]
    return partsupp.aggregate(stats)


def median_expr(partsupp, lineitem, t):
    stats = [partsupp.ps_supplycost.approx_median().name("MEDIAN_SUPPLYCOST")]
    return partsupp.aggregate(stats)


IBIS_AGGREGATE = {
    "sum": sum_expr,
    "avg": avg_expr,
    "min": min_expr,
    "max": max_expr,
    "median": median_expr,
}
