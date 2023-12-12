from typing import List, Optional

import ibis
import pandas as pd
import streamlit as st
from ibis import _

st.set_page_config(layout="wide")


def support_matrix_df():
    return (
        ibis.read_csv(
            "https://raw.githubusercontent.com/substrait-io/consumer-testing/main/app/consumer_results.csv"
        )
        .rename({"full_function": "FullFunction"})
        .mutate(
            function_category=_.full_function.split(".")[-2],
        )
        .execute()
    )


def backends_info_df():
    return pd.DataFrame(
        {
            "DataFusionProducer-DataFusionConsumer": [
                "DataFusionProducer",
                "DataFusionConsumer",
            ],
            "DataFusionProducer-DuckDBConsumer": [
                "DataFusionProducer",
                "DuckDBConsumer",
            ],
            "DuckDBProducer-DataFusionConsumer": [
                "DuckDBProducer",
                "DataFusionConsumer",
            ],
            "DuckDBProducer-DuckDBConsumer": ["DuckDBProducer", "DuckDBConsumer"],
            "IbisProducer-DuckDBConsumer": ["IbisProducer", "DuckDBConsumer"],
            "IbisProducer-DataFusionConsumer": ["IbisProducer", "DataFusionConsumer"],
            "IsthmusProducer-DuckDBConsumer": ["IsthmusProducer", "DuckDBConsumer"],
            "IsthmusProducer-DataFusionConsumer": [
                "IsthmusProducer",
                "DataFusionConsumer",
            ],
        }.items(),
        columns=["backend_name", "categories"],
    )


backend_info_table = ibis.memtable(backends_info_df())
support_matrix_table = ibis.memtable(support_matrix_df())


def get_all_producers():
    full_list = (
        backend_info_table.select(category=_.categories.unnest())
        .distinct()
        .order_by("category")["category"]
        .execute()
        .tolist()
    )
    producers_list = [x for x in full_list if "Producer" in x]
    return producers_list


def get_all_consumers():
    full_list = (
        backend_info_table.select(category=_.categories.unnest())
        .distinct()
        .order_by("category")["category"]
        .execute()
        .tolist()
    )
    consumers_list = [x for x in full_list if "Consumer" in x]
    return consumers_list


def get_all_function_categories():
    return (
        support_matrix_table.select(_.function_category)
        .distinct()["function_category"]
        .execute()
        .tolist()
    )


def get_backend_names(categories: Optional[List[str]] = None):
    backend_expr = backend_info_table.mutate(category=_.categories.unnest())
    if categories:
        backend_expr = backend_expr.filter(_.category.isin(categories))
    return (
        backend_expr.select(_.backend_name).distinct().backend_name.execute().tolist()
    )


def get_selected_producers():
    producers = get_all_producers()
    selected_categories_names = st.sidebar.multiselect(
        "Producers",
        options=producers,
        default=None,
    )
    if not selected_categories_names:
        return get_backend_names()
    return get_backend_names(selected_categories_names)


def get_selected_consumers():
    consumers = get_all_consumers()
    selected_categories_names = st.sidebar.multiselect(
        "Consumers",
        options=consumers,
        default=None,
    )
    if not selected_categories_names:
        return get_backend_names()
    return get_backend_names(selected_categories_names)


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


current_producers = get_selected_producers()
current_consumers = get_selected_consumers()
current_ops_categories = get_selected_function_categories()


# Start ibis expression
table_expr = support_matrix_table

# Add index to result
table_expr = table_expr.mutate(index=_.full_function)
table_expr = table_expr.order_by(_.index)

# Filter functions by selected categories
table_expr = table_expr.filter(_.function_category.isin(current_ops_categories))


def intersection(lst1, lst2):
    lst3 = [value for value in lst1 if value in lst2]
    return lst3


# Show only selected backend
current_producers_consumers = intersection(current_producers, current_consumers)
table_expr = table_expr[current_producers_consumers + ["index"]]

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
        .map(lambda n: f"{n} ({round(100 * n / all_visible_ops_count)}%)")
        .to_frame(name="API Coverage")
        .T
    )

    table = pd.concat([coverage, df.replace({True: "✔", False: "🚫"})]).loc[
        :, sorted(df.columns)
    ]
    st.dataframe(table)
else:
    st.write("No data")
