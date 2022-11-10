SQL_SCALAR = {
    "ceil":
        """
        SELECT PS_SUPPLYCOST, ceil(PS_SUPPLYCOST) AS CEIL_SUPPLYCOST
        FROM '{}';
        """,
    "floor":
        """
        SELECT PS_SUPPLYCOST, floor(PS_SUPPLYCOST) AS FLOOR_SUPPLYCOST
        FROM '{}';
        """,
}
