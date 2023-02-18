from pathlib import Path
import pyarrow as pa


REPO_DIR = Path(__file__).parent.parent
schema_file = Path.joinpath(REPO_DIR, "tests/data/schema.sql")

ORDERS_TABLE = """
CREATE TABLE orders(
    o_orderkey INTEGER NOT NULL,
    o_custkey INTEGER NOT NULL,
    o_orderstatus VARCHAR NOT NULL,
    o_totalprice DOUBLE NOT NULL,
    o_orderdate DATE NOT NULL,
    o_orderpriority VARCHAR NOT NULL,
    o_clerk VARCHAR NOT NULL,
    o_shippriority INTEGER NOT NULL,
    o_comment VARCHAR NOT NULL);
"""
ORDERS_PA_SCHEMA = pa.schema([
    pa.field('o_orderkey', pa.int32()),
    pa.field('o_custkey', pa.int32()),
    pa.field('o_orderstatus', pa.string()),
    pa.field('o_totalprice', pa.float64()),
    pa.field('o_orderdate', pa.date32()),
])
LINEITEM_TABLE = """
CREATE TABLE lineitem(
    l_orderkey INTEGER NOT NULL,
    l_partkey INTEGER NOT NULL,
    l_suppkey INTEGER NOT NULL,
    l_linenumber INTEGER NOT NULL,
    l_quantity DOUBLE NOT NULL,
    l_extendedprice DOUBLE NOT NULL,
    l_discount DOUBLE NOT NULL,
    l_tax DOUBLE NOT NULL,
    l_returnflag VARCHAR NOT NULL,
    l_linestatus VARCHAR NOT NULL,
    l_shipdate DATE NOT NULL,
    l_commitdate DATE NOT NULL,
    l_receiptdate DATE NOT NULL,
    l_shipinstruct VARCHAR NOT NULL,
    l_shipmode VARCHAR NOT NULL,
    l_comment VARCHAR NOT NULL);
"""
LINEITEM_PA_SCHEMA = pa.schema([
    pa.field('l_orderkey', pa.int32()),
    pa.field('l_partkey', pa.int32()),
    pa.field('l_suppkey', pa.int32()),
    pa.field('l_linenumber', pa.int32()),
    pa.field('l_quantity', pa.float64()),
    pa.field('l_extendedprice', pa.float64()),
    pa.field('l_discount', pa.float64()),
    pa.field('l_tax', pa.float64()),
    pa.field('l_returnflag', pa.string()),
    pa.field('l_linestatus', pa.string()),
    pa.field('l_shipdate', pa.date32()),
    pa.field('l_commitdate', pa.date32()),
    pa.field('l_receiptdate', pa.date32()),
    pa.field('l_shipinstruct', pa.string()),
    pa.field('l_shipmode', pa.string()),
    pa.field('l_comment', pa.string()),
])
PARTSUPP_TABLE = """
CREATE TABLE partsupp(
    ps_partkey INTEGER NOT NULL,
    ps_suppkey INTEGER NOT NULL,
    ps_availqty INTEGER NOT NULL,
    ps_supplycost DOUBLE NOT NULL,
    ps_comment VARCHAR NOT NULL);
"""
PARTSUPP_PA_SCHEMA = pa.schema([
    pa.field('ps_partkey', pa.int32()),
    pa.field('ps_suppkey', pa.int32()),
    pa.field('ps_availqty', pa.int32()),
    pa.field('ps_supplycost', pa.float64()),
    pa.field('ps_comment', pa.string()),
])
PART_TABLE = """
CREATE TABLE part(
    p_partkey INTEGER NOT NULL,
    p_name VARCHAR NOT NULL,
    p_mfgr VARCHAR NOT NULL,
    p_brand VARCHAR NOT NULL,
    p_type VARCHAR NOT NULL,
    p_size INTEGER NOT NULL,
    p_container VARCHAR NOT NULL,
    p_retailprice DOUBLE NOT NULL,
    p_comment VARCHAR NOT NULL);
"""
PART_PA_SCHEMA = pa.schema([
    pa.field('ps_partkey', pa.int32()),
    pa.field('p_name', pa.string()),
    pa.field('p_mfgr', pa.string()),
    pa.field('p_brand', pa.string()),
    pa.field('p_type', pa.string()),
    pa.field('p_size', pa.int32()),
    pa.field('p_container', pa.string()),
    pa.field('p_retailprice', pa.float64()),
    pa.field('p_comment', pa.string()),
])
CUSTOMER_TABLE = """
CREATE TABLE customer(
    c_custkey INTEGER NOT NULL,
    c_name VARCHAR NOT NULL,
    c_address VARCHAR NOT NULL,
    c_nationkey INTEGER NOT NULL,
    c_phone VARCHAR NOT NULL,
    c_acctbal DOUBLE NOT NULL,
    c_mktsegment VARCHAR NOT NULL,
    c_comment VARCHAR NOT NULL);
"""
CUSTOMER_PA_SCHEMA = pa.schema([
    pa.field('c_custkey', pa.int32()),
    pa.field('c_name', pa.string()),
    pa.field('c_address', pa.string()),
    pa.field('c_nationkey', pa.int32()),
    pa.field('c_phone', pa.string()),
    pa.field('c_acctbal', pa.float64()),
    pa.field('c_mktsegment', pa.string()),
    pa.field('c_comment', pa.string()),
])
SUPPLIER_TABLE = """
    CREATE TABLE supplier(
    s_suppkey INTEGER NOT NULL,
    s_name VARCHAR NOT NULL,
    s_address VARCHAR NOT NULL,
    s_nationkey INTEGER NOT NULL,
    s_phone VARCHAR NOT NULL,
    s_acctbal DOUBLE NOT NULL,
    s_comment VARCHAR NOT NULL);
"""
CUSTOMER_PA_SCHEMA = pa.schema([
    pa.field('s_suppkey', pa.int32()),
    pa.field('s_name', pa.string()),
    pa.field('s_address', pa.string()),
    pa.field('s_nationkey', pa.int32()),
    pa.field('s_phone', pa.string()),
    pa.field('s_acctbal', pa.float64()),
    pa.field('c_comment', pa.string()),
])
TABLE_TO_RECREATE = {
    "orders": ORDERS_TABLE,
    "lineitem": LINEITEM_TABLE,
    "partsupp": PARTSUPP_TABLE,
    "part": PART_TABLE,
    "customer": CUSTOMER_TABLE,
    "supplier": SUPPLIER_TABLE,
}
PA_SCHEMA = {
    "orders": ORDERS_PA_SCHEMA,
    "lineitem": LINEITEM_PA_SCHEMA,
    "partsupp": PARTSUPP_PA_SCHEMA,
    "part": PART_PA_SCHEMA,
    "customer": CUSTOMER_PA_SCHEMA,
    "supplier": PARTSUPP_PA_SCHEMA,
    "nation": None,
    "region": None
}
