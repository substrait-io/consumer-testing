def ln_expr(partsupp):
    new_col = partsupp.ps_supplycost.ln().name("LN_SUPPLY")
    return partsupp[partsupp.ps_supplycost, new_col]


def log10_expr(partsupp):
    new_col = partsupp.ps_supplycost.log10().name("LOG10_SUPPLY")
    return partsupp[partsupp.ps_supplycost, new_col]


def log2_expr(partsupp):
    new_col = partsupp.ps_supplycost.log2().name("LOG2_SUPPLY")
    return partsupp[partsupp.ps_supplycost, new_col]


def logb_expr(partsupp):
    new_col = partsupp.ps_supplycost.log(3).name("LOGB_3_SUPPLY")
    return partsupp[partsupp.ps_supplycost, new_col]


IBIS_SCALAR = {
    "ln": ln_expr,
    "log10": log10_expr,
    "log2": log2_expr,
    "logb": logb_expr,
}
