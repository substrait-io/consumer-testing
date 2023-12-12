from typing import List, Optional

import ibis
import pandas as pd
import streamlit as st
from ibis import _

st.set_page_config(layout="wide")


def support_matrix_df():
    return (
        ibis.read_csv(
            "https://raw.githubusercontent.com/substrait-io/consumer-testing/main/app/producer_results.csv"
        )
        .rename({"full_operation": "FullFunction"})
        .mutate(
            function_category=_.full_operation.split(".")[-2],
        )
        .execute()
    )


def producer_info_df():
    return pd.DataFrame(
        {
            "DataFusionProducer": ["DataFusionProducer", "sql"],
            "DuckDBProducer": ["DuckDBProducer", "sql"],
            "IbisProducer": ["IbisProducer", "expression"],
            "IsthmusProducer": ["IsthmusProducer", "sql"],
        }.items(),
        columns=["producer_name", "categories"],
    )


producer_info_table = ibis.memtable(producer_info_df())
support_matrix_table = ibis.memtable(support_matrix_df())


def get_all_function_categories():
    return (
        support_matrix_table.select(_.function_category)
        .distinct()["function_category"]
        .execute()
        .tolist()
    )


def get_producer_names(categories: Optional[List[str]] = None):
    producer_expr = producer_info_table.mutate(category=_.categories.unnest())
    if categories:
        producer_expr = producer_expr.filter(_.category.isin(categories))
    return (
        producer_expr.select(_.producer_name)
        .distinct()
        .producer_name.execute()
        .tolist()
    )


def get_selected_function_categories():
    all_ops_categories = get_all_function_categories()

    selected_ops_categories = st.sidebar.multiselect(
        "Function category",
        options=sorted(all_ops_categories),
        default=None,
    )
    if not selected_ops_categories:
        selected_ops_categories = all_ops_categories
    return selected_ops_categories


def get_selected_producers():
    producers = get_producer_names()
    selected_categories_names = st.sidebar.multiselect(
        "Producers",
        options=producers,
        default=None,
    )
    if not selected_categories_names:
        return get_producer_names()
    return get_producer_names(selected_categories_names)


current_producer_names = get_selected_producers()
current_ops_categories = get_selected_function_categories()

# Start ibis expression
table_expr = support_matrix_table

# Add index to result
table_expr = table_expr.mutate(index=_.full_operation)
table_expr = table_expr.order_by(_.index)

# Filter functions by selected categories
table_expr = table_expr.filter(_.function_category.isin(current_ops_categories))

# Show only selected producer
table_expr = table_expr[current_producer_names + ["index"]]

# Execute query
df = table_expr.execute()
df = df.set_index("index")

# Display result
all_visible_ops_count = len(df.index)
if all_visible_ops_count:
    # Compute coverage
    coverage = (
        df.sum()
        .sort_values(ascending=False)
        .map(
            lambda n: f"{n}/{all_visible_ops_count} ({round(100 * n / all_visible_ops_count)}%)"
        )
        .to_frame(name="API Coverage")
        .T
    )

    table = pd.concat([coverage, df.replace({True: "✔", False: "🚫"})]).loc[
        :, sorted(df.columns)
    ]
    st.dataframe(table)
else:
    st.write("No data")
