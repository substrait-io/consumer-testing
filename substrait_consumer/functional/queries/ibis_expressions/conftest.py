import ibis
from ibis_substrait.tests.compiler.conftest import *


@pytest.fixture
def t():
    return ibis.table(
        [("a", dt.int32), ("b", dt.int32), ("c", dt.boolean), ("d", dt.boolean)],
        name="t",
    )
