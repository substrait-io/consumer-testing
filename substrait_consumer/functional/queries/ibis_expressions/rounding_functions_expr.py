def ceil_expr(partsupp):
    new_col = partsupp.ps_supplycost.ceil().name("CEIL_SUPPLYCOST")
    return partsupp[new_col]


def floor_expr(partsupp):
    new_col = partsupp.ps_supplycost.floor().name("FLOOR_SUPPLYCOST")
    return partsupp[new_col]


IBIS_SCALAR = {
    "ceil": ceil_expr,
    "floor": floor_expr,
}
