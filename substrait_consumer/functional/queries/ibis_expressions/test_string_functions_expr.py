from substrait_consumer.functional.common import substrait_producer_ibis_test


@substrait_producer_ibis_test("function", "string")
def test_concat_expr(nation):
    new_col = nation.n_name.concat(nation.n_comment).name("concat_nation")
    return nation[nation.n_name, new_col]


@substrait_producer_ibis_test("function", "string")
def test_starts_with0_expr(nation):
    proj = nation.projection(["n_name"])
    filtered = proj.filter(nation.n_name.startswith("A"))
    return filtered


@substrait_producer_ibis_test("function", "string")
def test_ends_with_expr(nation):
    proj = nation.projection(["n_name"])
    filtered = proj.filter(nation.n_name.endswith("A"))
    return filtered


@substrait_producer_ibis_test("function", "string")
def test_lower_expr(nation):
    new_col = nation.n_name.lower().name("lowercase_N_NAME")
    return nation[nation.n_name, new_col]


@substrait_producer_ibis_test("function", "string")
def test_upper_expr(orders):
    limited = orders.limit(10)
    new_col = limited.o_comment.upper().name("uppercase_O_COMMENT")
    return limited[limited.o_comment, new_col]


@substrait_producer_ibis_test("function", "string")
def test_substring0_expr(nation):
    new_col = nation.n_name.substr(1, 3).name("substr_name")
    return nation[nation.n_name, new_col]


@substrait_producer_ibis_test("function", "string")
def test_contains_expr(nation):
    proj = nation.projection(["n_name"])
    filtered = proj.filter(nation.n_name.contains("IA"))
    return filtered


@substrait_producer_ibis_test("function", "string")
def test_replace_expr(nation):
    new_col = nation.n_name.replace("A", "a").name("replace_name")
    return nation[nation.n_name, new_col]


@substrait_producer_ibis_test("function", "string")
def test_repeat_expr(nation):
    new_col = nation.n_name.repeat(2).name("repeated_N_NAME")
    return nation[nation.n_name, new_col]


@substrait_producer_ibis_test("function", "string")
def test_reverse_expr(nation):
    new_col = nation.n_name.reverse().name("reversed_N_NAME")
    return nation[nation.n_name, new_col]


@substrait_producer_ibis_test("function", "string")
def test_char_length_expr(nation):
    new_col = nation.n_name.length().name("char_length_N_NAME")
    return nation[nation.n_name, new_col]


@substrait_producer_ibis_test("function", "string")
def test_left_expr(nation):
    new_col = nation.n_name.left(3).name("left_extract_N_NAME")
    return nation[nation.n_name, new_col]


@substrait_producer_ibis_test("function", "string")
def test_right_expr(nation):
    new_col = nation.n_name.right(2).name("right_extract_N_NAME")
    return nation[nation.n_name, new_col]


@substrait_producer_ibis_test("function", "string")
def test_lpad_expr(nation):
    new_col = nation.n_name.lpad(10, " ").name("lpad_N_NAME")
    return nation[nation.n_name, new_col]


@substrait_producer_ibis_test("function", "string")
def test_rpad_expr(nation):
    new_col = nation.n_name.rpad(10, " ").name("rpad_N_NAME")
    return nation[nation.n_name, new_col]


@substrait_producer_ibis_test("function", "string")
def test_ltrim_expr(nation):
    new_col = nation.n_name.lstrip().name("ltrim_N_NAME")
    return nation[nation.n_name, new_col]


@substrait_producer_ibis_test("function", "string")
def test_rtrim_expr(nation):
    new_col = nation.n_name.rstrip().name("rtrim_N_NAME")
    return nation[nation.n_name, new_col]


@substrait_producer_ibis_test("function", "string")
def test_trim_expr(nation):
    new_col = nation.n_name.strip().name("trim_N_NAME")
    return nation[nation.n_name, new_col]


@substrait_producer_ibis_test("function", "string")
def test_strpos_expr(nation):
    new_col = nation.n_name.find("A").name("strpos_name")
    return nation[nation.n_name, new_col]
