def equal_expr(partsupp, nation):
    proj = partsupp.projection(['ps_availqty', 'ps_partkey'])
    filtered = proj.filter(partsupp.ps_availqty == partsupp.ps_partkey)
    return filtered


def not_equal_expr(partsupp, nation):
    proj = nation.projection(['n_name'])
    filtered = proj.filter(nation.n_name != 'CANADA')
    return filtered


def lt_expr(partsupp, nation):
    proj = partsupp.projection(['ps_availqty'])
    filtered = proj.filter(partsupp.ps_availqty < 10)
    return filtered


def lte_expr(partsupp, nation):
    proj = partsupp.projection(['ps_availqty'])
    filtered = proj.filter(partsupp.ps_availqty <= 10)
    return filtered


def gt_expr(partsupp, nation):
    proj = partsupp.projection(['ps_availqty'])
    filtered = proj.filter(partsupp.ps_availqty > 10)
    return filtered


def gte_expr(partsupp, nation):
    proj = partsupp.projection(['ps_availqty'])
    filtered = proj.filter(partsupp.ps_availqty >= 10)
    return filtered


IBIS_SCALAR = {
    "equal": equal_expr,
    "not_equal": not_equal_expr,
    "lt": lt_expr,
    "lte": lte_expr,
    "gt": gt_expr,
    "gte": gte_expr
}