def and_expr(t):
    proj = t.projection(['a', 'b'])
    cond = (proj.a < 5) & (proj.b == 1)
    return proj[cond]


def or_expr(t):
    proj = t.projection(['a'])
    cond = (proj.a == 5) | (t.a == 7)
    return proj[cond]


IBIS_SCALAR = {
    "and": and_expr,
    "or": or_expr
}