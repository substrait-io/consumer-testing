def ceil_expr(partsupp, lineitem):
    limited = partsupp.limit(10)
    new_col = limited.ps_supplycost.ceil().name("CEIL_SUPPLYCOST")
    return limited[limited.ps_supplycost, new_col]


def floor_expr(partsupp, lineitem):
    limited = partsupp.limit(10)
    new_col = limited.ps_supplycost.floor().name("FLOOR_SUPPLYCOST")
    return limited[limited.ps_supplycost, new_col]


def round_expr(partsupp, lineitem):
    limited = lineitem.limit(10)
    new_col = limited.l_extendedprice.round(1).name("ROUND_EXTENDEDPRICE")
    return limited[limited.l_extendedprice, new_col]


IBIS_SCALAR = {
    "ceil": ceil_expr,
    "floor": floor_expr,
    "round": round_expr,
}
