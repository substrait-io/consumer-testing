def ln_expr(partsupp):
    limited = partsupp.limit(10)
    new_col = limited.ps_supplycost.ln().round(digits=2).name("LN_SUPPLY")
    return limited[limited.ps_supplycost, new_col]


def log10_expr(partsupp):
    limited = partsupp.limit(10)
    new_col = limited.ps_supplycost.log10().round(digits=2).name("LOG10_SUPPLY")
    return limited[limited.ps_supplycost, new_col]


def log2_expr(partsupp):
    limited = partsupp.limit(10)
    new_col = limited.ps_supplycost.log2().round(digits=2).name("LOG2_SUPPLY")
    return limited[limited.ps_supplycost, new_col]


def logb_expr(partsupp):
    limited = partsupp.limit(10)
    new_col = limited.ps_supplycost.log(3).round(2).name("LOGB_3_SUPPLY")
    return limited[limited.ps_supplycost, new_col]


IBIS_SCALAR = {
    "ln": ln_expr,
    "log10": log10_expr,
    "log2": log2_expr,
    "logb": logb_expr,
}
