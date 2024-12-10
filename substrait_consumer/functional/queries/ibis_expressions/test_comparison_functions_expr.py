from substrait_consumer.functional.common import substrait_producer_ibis_test


@substrait_producer_ibis_test("function", "comparison")
def test_equal_expr(partsupp):
    proj = partsupp.projection(["ps_availqty", "ps_partkey"])
    filtered = proj.filter(partsupp.ps_availqty == partsupp.ps_partkey)
    return filtered


@substrait_producer_ibis_test("function", "comparison")
def test_not_equal_expr(nation):
    proj = nation.projection(["n_name"])
    filtered = proj.filter(nation.n_name != "CANADA")
    return filtered


@substrait_producer_ibis_test("function", "comparison")
def test_lt_expr(partsupp):
    proj = partsupp.projection(["ps_availqty"])
    filtered = proj.filter(partsupp.ps_availqty < 10)
    return filtered


@substrait_producer_ibis_test("function", "comparison")
def test_lte_expr(partsupp):
    proj = partsupp.projection(["ps_availqty"])
    filtered = proj.filter(partsupp.ps_availqty <= 10)
    return filtered


@substrait_producer_ibis_test("function", "comparison")
def test_gt_expr(partsupp):
    proj = partsupp.projection(["ps_availqty"])
    filtered = proj.filter(partsupp.ps_availqty > 10)
    return filtered


@substrait_producer_ibis_test("function", "comparison")
def test_gte_expr(partsupp):
    proj = partsupp.projection(["ps_availqty"])
    filtered = proj.filter(partsupp.ps_availqty >= 10)
    return filtered
