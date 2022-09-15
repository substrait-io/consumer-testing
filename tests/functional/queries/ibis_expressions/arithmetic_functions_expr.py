def add_expr(partsupp):
    new_col = (partsupp.ps_partkey + partsupp.ps_suppkey).name('ADD_KEY')
    return partsupp[partsupp.ps_partkey, partsupp.ps_suppkey, new_col]


def subtract_expr(partsupp):
    new_col = (partsupp.ps_partkey - partsupp.ps_suppkey).name('SUBSTRACT_KEY')
    return partsupp[partsupp.ps_partkey, partsupp.ps_suppkey, new_col]


def multiply_expr(partsupp):
    new_col = (partsupp.ps_partkey * 10).name('MULTIPLY_KEY')
    return partsupp[partsupp.ps_partkey, new_col]


def divide_expr(partsupp):
    new_col = (partsupp.ps_partkey / 10).name('DIVIDE_KEY')
    return partsupp[partsupp.ps_partkey, new_col]


IBIS_SCALAR = {
    "add": add_expr,
    "subtract": subtract_expr,
    "multiply": multiply_expr,
    "divide": divide_expr
}
