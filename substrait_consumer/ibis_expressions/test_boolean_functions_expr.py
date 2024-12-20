from substrait_consumer.functional.common import substrait_producer_ibis_test


@substrait_producer_ibis_test("function", "boolean")
def test_and_expr(t):
    proj = t.projection(["a", "b"])
    cond = (proj.a < 5) & (proj.b == 1)
    return proj[cond]


@substrait_producer_ibis_test("function", "boolean")
def test_or_expr(t):
    proj = t.projection(["a"])
    cond = (proj.a == 5) | (t.a == 7)
    return proj[cond]
