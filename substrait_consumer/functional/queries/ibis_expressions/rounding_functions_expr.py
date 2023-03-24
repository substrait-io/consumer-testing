def ceil_expr(partsupp, lineitem):
    new_col = partsupp.ps_supplycost.ceil().name("CEIL_SUPPLYCOST")
    return partsupp[partsupp.ps_supplycost, new_col]


def floor_expr(partsupp, lineitem):
    new_col = partsupp.ps_supplycost.floor().name("FLOOR_SUPPLYCOST")
    return partsupp[partsupp.ps_supplycost, new_col]


def round_expr(partsupp, lineitem):
    new_col = lineitem.l_extendedprice.round(1).name("ROUND_EXTENDEDPRICE")
    return lineitem[lineitem.l_extendedprice, new_col]


IBIS_SCALAR = {
    "ceil": ceil_expr,
    "floor": floor_expr,
    "round": round_expr,
}
