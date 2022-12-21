SELECT
  l_returnflag,
  l_linestatus,
  sum(l_quantity) AS sum_qty,
  round(sum(l_extendedprice), 2) AS sum_base_price,
  round(sum(l_extendedprice * (1 - l_discount)), 2) AS sum_disc_price,
  round(sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)), 2) AS sum_charge,
  round(avg(l_quantity), 2) AS avg_qty,
  round(avg(l_extendedprice), 2) AS avg_price,
  round(avg(l_discount), 2) AS avg_disc,
  count(*) AS count_order
FROM
  '{}'
WHERE
  l_shipdate <= date '1998-12-01' - interval '120' day
GROUP BY
  l_returnflag,
  l_linestatus
ORDER BY
  l_returnflag,
  l_linestatus
