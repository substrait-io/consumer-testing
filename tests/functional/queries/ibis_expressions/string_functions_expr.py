def concat_expr(nation, orders):
    new_col = nation.n_name.concat(nation.n_comment).name("concat_nation")
    return nation[nation.n_name, new_col]


def starts_with_expr(nation, orders):
    proj = nation.projection(["n_name"])
    filtered = proj.filter(nation.n_name.startswith("A"))
    return filtered


def ends_with_expr(nation, orders):
    proj = nation.projection(["n_name"])
    filtered = proj.filter(nation.n_name.endswith("A"))
    return filtered


def lower_expr(nation, orders):
    new_col = nation.n_name.lower().name("lowercase_N_NAME")
    return nation[nation.n_name, new_col]


def upper_expr(nation, orders):
    new_col = orders.o_comment.upper().name("uppercase_O_COMMENT")
    return orders[orders.o_comment, new_col]


def substr_expr(nation, orders):
    new_col = nation.n_name.substr(1, 3).name("substr_name")
    return nation[nation.n_name, new_col]


def contains_expr(nation, orders):
    proj = nation.projection(["n_name"])
    filtered = proj.filter(nation.n_name.contains("IA"))
    return filtered


def replace_expr(nation, orders):
    new_col = nation.n_name.replace("A", "a").name("replace_name")
    return nation[nation.n_name, new_col]


def repeat_expr(nation, orders):
    new_col = nation.n_name.repeat(2).name("repeated_N_NAME")
    return nation[nation.n_name, new_col]


def reverse_expr(nation, orders):
    new_col = nation.n_name.reverse().name("reversed_N_NAME")
    return nation[nation.n_name, new_col]


def char_length_expr(nation, orders):
    new_col = nation.n_name.length().name("char_length_N_NAME")
    return nation[nation.n_name, new_col]


def left_expr(nation, orders):
    new_col = nation.n_name.left(3).name("left_extract_N_NAME")
    return nation[nation.n_name, new_col]


def right_expr(nation, orders):
    new_col = nation.n_name.right(2).name("right_extract_N_NAME")
    return nation[nation.n_name, new_col]


def lpad_expr(nation, orders):
    new_col = nation.n_name.lpad(10, " ").name("lpad_N_NAME")
    return nation[nation.n_name, new_col]


def rpad_expr(nation, orders):
    new_col = nation.n_name.rpad(10, " ").name("rpad_N_NAME")
    return nation[nation.n_name, new_col]


def ltrim_expr(nation, orders):
    new_col = nation.n_name.lstrip("A").name("ltrim_N_NAME")
    return nation[nation.n_name, new_col]


def rtrim_expr(nation, orders):
    new_col = nation.n_name.rstrip("A").name("rtrim_N_NAME")
    return nation[nation.n_name, new_col]


def trim_expr(nation, orders):
    new_col = nation.n_name.strip("A").name("trim_N_NAME")
    return nation[nation.n_name, new_col]


def strpos_expr(nation, orders):
    new_col = nation.n_name.find("A").name("strpos_name")
    return nation[nation.n_name, new_col]


IBIS_SCALAR = {
    "concat": concat_expr,
    "starts_with": starts_with_expr,
    "ends_with": ends_with_expr,
    "lower": lower_expr,
    "upper": upper_expr,
    "substr": substr_expr,
    "contains": contains_expr,
    "replace": replace_expr,
    "repeat": repeat_expr,
    "reverse": reverse_expr,
    "char_length": char_length_expr,
    "left": left_expr,
    "right": right_expr,
    "lpad": lpad_expr,
    "rpad": rpad_expr,
    "ltrim": ltrim_expr,
    "rtrim": rtrim_expr,
    "trim": trim_expr,
    "strpos": strpos_expr,
}
