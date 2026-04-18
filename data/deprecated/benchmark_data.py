from __future__ import annotations

DATASET_SCHEMAS = {
    "customers": {
        "file_name": "customers.csv",
        "format": "csv",
        "schema": {
            "customer_id": "int64",
            "country": "string",
            "city": "string",
            "signup_date": "date",
            "segment": "string",
            "age": "int32",
            "is_premium": "bool",
            "churned": "bool",
            "lifetime_value": "float64"
        }
    },
    "orders": {
        "file_name": "orders.csv",
        "format": "csv",
        "schema": {
            "order_id": "int64",
            "customer_id": "int64",
            "order_date": "datetime",
            "status": "string",
            "channel": "string",
            "currency": "string",
            "amount": "float64",
            "discount_amount": "float64",
            "payment_delay_days": "int32"
        }
    },
    "order_items": {
        "file_name": "order_items.csv",
        "format": "csv",
        "schema": {
            "order_item_id": "int64",
            "order_id": "int64",
            "product_id": "int64",
            "quantity": "int32",
            "unit_price": "float64",
            "item_discount": "float64",
            "warehouse_id": "int32"
        }
    },
    "products": {
        "file_name": "products.csv",
        "format": "csv",
        "schema": {
            "product_id": "int64",
            "category": "string",
            "sub_category": "string",
            "brand": "string",
            "launch_date": "date",
            "is_active": "bool",
            "base_price": "float64",
            "weight_grams": "float64"
        }
    },
    "events": {
        "file_name": "events.csv",
        "format": "csv",
        "schema": {
            "event_id": "int64",
            "customer_id": "int64",
            "session_id": "string",
            "event_time": "datetime",
            "event_type": "string",
            "device": "string",
            "source": "string",
            "page": "string",
            "duration_sec": "float64"
        }
    }
}

QUESTIONS = [
    {
        "id": "q01",
        "datasets": [
            "customers"
        ],
        "question": "Count premium customers by country from customers."
    },
    {
        "id": "q02",
        "datasets": [
            "orders"
        ],
        "question": "Compute total revenue and average discount_amount for completed orders grouped by channel from orders."
    },
    {
        "id": "q03",
        "datasets": [
            "products"
        ],
        "question": "Find the top 10 brands by average base_price among active products from products."
    },
    {
        "id": "q04",
        "datasets": [
            "events"
        ],
        "question": "Count events grouped by event_type and device from events."
    },
    {
        "id": "q05",
        "datasets": [
            "order_items"
        ],
        "question": "Compute total quantity and gross sales per product_id from order_items where gross_sales = quantity * unit_price."
    },
    {
        "id": "q06",
        "datasets": [
            "customers",
            "orders"
        ],
        "question": "For each customer segment, compute completed order count, total revenue, and average order amount using customers and orders."
    },
    {
        "id": "q07",
        "datasets": [
            "order_items",
            "products"
        ],
        "question": "Compute net sales by category using order_items and products where net_sales = quantity * unit_price - item_discount."
    },
    {
        "id": "q08",
        "datasets": [
            "customers",
            "events"
        ],
        "question": "Compute average number of sessions per customer grouped by is_premium using customers and events."
    },
    {
        "id": "q09",
        "datasets": [
            "orders"
        ],
        "question": "Compute daily revenue for completed orders and a 7-day rolling average from orders."
    },
    {
        "id": "q10",
        "datasets": [
            "customers",
            "orders"
        ],
        "question": "Find top 20 customers by lifetime_value with at least 5 completed orders using customers and orders."
    },
    {
        "id": "q11",
        "datasets": [
            "products",
            "order_items"
        ],
        "question": "Compute brand share of quantity sold within each category using products and order_items."
    },
    {
        "id": "q12",
        "datasets": [
            "events"
        ],
        "question": "Compute time difference in seconds between consecutive events per customer from events."
    },
    {
        "id": "q13",
        "datasets": [
            "customers",
            "orders",
            "events"
        ],
        "question": "Compute conversion rate per country as customers with completed orders divided by customers with events using customers, orders, and events."
    },
    {
        "id": "q14",
        "datasets": [
            "products",
            "order_items"
        ],
        "question": "Find top 3 products by net sales within each category using products and order_items."
    },
    {
        "id": "q15",
        "datasets": [
            "customers",
            "orders"
        ],
        "question": "Compute median order amount and 90th percentile payment_delay_days per segment and month using customers and orders."
    }
]

SOLUTIONS = {
    'q01': 'import polars as pl\n\ncustomers = pl.scan_csv("data/customers.csv", try_parse_dates=True)\n\nresult = (\n    customers\n    .filter(pl.col("is_premium"))\n    .group_by("country")\n    .agg(pl.len().alias("premium_customers"))\n    .sort(["premium_customers", "country"], descending=[True, False])\n    .collect()\n)\n',    'q02': 'import polars as pl\n\norders = pl.scan_csv("data/orders.csv", try_parse_dates=True)\n\nresult = (\n    orders\n    .filter(pl.col("status") == "completed")\n    .group_by("channel")\n    .agg(\n        pl.col("amount").sum().alias("total_revenue"),\n        pl.col("discount_amount").mean().alias("avg_discount_amount"),\n    )\n    .sort("channel")\n    .collect()\n)\n',    'q03': 'import polars as pl\n\nproducts = pl.scan_csv("data/products.csv", try_parse_dates=True)\n\nresult = (\n    products\n    .filter(pl.col("is_active"))\n    .group_by("brand")\n    .agg(pl.col("base_price").mean().alias("avg_base_price"))\n    .sort("avg_base_price", descending=True)\n    .head(10)\n    .collect()\n)\n',    'q04': 'import polars as pl\n\nevents = pl.scan_csv("data/events.csv", try_parse_dates=True)\n\nresult = (\n    events\n    .group_by(["event_type", "device"])\n    .agg(pl.len().alias("event_count"))\n    .sort(["event_type", "device"])\n    .collect()\n)\n',    'q05': 'import polars as pl\n\norder_items = pl.scan_csv("data/order_items.csv", try_parse_dates=True)\n\nresult = (\n    order_items\n    .with_columns((pl.col("quantity") * pl.col("unit_price")).alias("gross_sales"))\n    .group_by("product_id")\n    .agg(\n        pl.col("quantity").sum().alias("total_quantity"),\n        pl.col("gross_sales").sum().alias("gross_sales"),\n    )\n    .sort("product_id")\n    .collect()\n)\n',    'q06': 'import polars as pl\n\ncustomers = pl.scan_csv("data/customers.csv", try_parse_dates=True)\norders = pl.scan_csv("data/orders.csv", try_parse_dates=True)\n\nresult = (\n    orders\n    .filter(pl.col("status") == "completed")\n    .join(customers.select(["customer_id", "segment"]), on="customer_id", how="inner")\n    .group_by("segment")\n    .agg(\n        pl.len().alias("completed_order_count"),\n        pl.col("amount").sum().alias("total_revenue"),\n        pl.col("amount").mean().alias("avg_order_amount"),\n    )\n    .sort("segment")\n    .collect()\n)\n',    'q07': 'import polars as pl\n\norder_items = pl.scan_csv("data/order_items.csv", try_parse_dates=True)\nproducts = pl.scan_csv("data/products.csv", try_parse_dates=True)\n\nresult = (\n    order_items\n    .join(products.select(["product_id", "category"]), on="product_id", how="inner")\n    .with_columns((pl.col("quantity") * pl.col("unit_price") - pl.col("item_discount")).alias("net_sales"))\n    .group_by("category")\n    .agg(pl.col("net_sales").sum().alias("net_sales"))\n    .sort("category")\n    .collect()\n)\n',    'q08': 'import polars as pl\n\ncustomers = pl.scan_csv("data/customers.csv", try_parse_dates=True)\nevents = pl.scan_csv("data/events.csv", try_parse_dates=True)\n\nsessions_per_customer = (\n    events\n    .group_by("customer_id")\n    .agg(pl.col("session_id").n_unique().alias("session_count"))\n)\n\nresult = (\n    customers\n    .join(sessions_per_customer, on="customer_id", how="left")\n    .with_columns(pl.col("session_count").fill_null(0))\n    .group_by("is_premium")\n    .agg(pl.col("session_count").mean().alias("avg_sessions_per_customer"))\n    .sort("is_premium")\n    .collect()\n)\n',    'q09': 'import polars as pl\n\norders = pl.scan_csv("data/orders.csv", try_parse_dates=True)\n\ndaily = (\n    orders\n    .filter(pl.col("status") == "completed")\n    .with_columns(pl.col("order_date").dt.date().alias("order_day"))\n    .group_by("order_day")\n    .agg(pl.col("amount").sum().alias("daily_revenue"))\n    .sort("order_day")\n)\n\nresult = (\n    daily\n    .with_columns(\n        pl.col("daily_revenue")\n        .rolling_mean(window_size=7, min_periods=1)\n        .alias("rolling_7d_avg_revenue")\n    )\n    .collect()\n)\n',    'q10': 'import polars as pl\n\ncustomers = pl.scan_csv("data/customers.csv", try_parse_dates=True)\norders = pl.scan_csv("data/orders.csv", try_parse_dates=True)\n\ncompleted_counts = (\n    orders\n    .filter(pl.col("status") == "completed")\n    .group_by("customer_id")\n    .agg(pl.len().alias("completed_orders"))\n    .filter(pl.col("completed_orders") >= 5)\n)\n\nresult = (\n    completed_counts\n    .join(customers.select(["customer_id", "lifetime_value"]), on="customer_id", how="inner")\n    .sort("lifetime_value", descending=True)\n    .head(20)\n    .collect()\n)\n',    'q11': 'import polars as pl\n\nproducts = pl.scan_csv("data/products.csv", try_parse_dates=True)\norder_items = pl.scan_csv("data/order_items.csv", try_parse_dates=True)\n\nbrand_qty = (\n    order_items\n    .join(products.select(["product_id", "category", "brand"]), on="product_id", how="inner")\n    .group_by(["category", "brand"])\n    .agg(pl.col("quantity").sum().alias("brand_quantity"))\n)\n\ncategory_qty = (\n    brand_qty\n    .group_by("category")\n    .agg(pl.col("brand_quantity").sum().alias("category_quantity"))\n)\n\nresult = (\n    brand_qty\n    .join(category_qty, on="category", how="inner")\n    .with_columns((pl.col("brand_quantity") / pl.col("category_quantity")).alias("quantity_share"))\n    .sort(["category", "brand"])\n    .collect()\n)\n',    'q12': 'import polars as pl\n\nevents = pl.scan_csv("data/events.csv", try_parse_dates=True)\n\nresult = (\n    events\n    .sort(["customer_id", "event_time"])\n    .with_columns(\n        pl.col("event_time").shift(1).over("customer_id").alias("prev_event_time")\n    )\n    .with_columns(\n        (pl.col("event_time") - pl.col("prev_event_time")).dt.total_seconds().alias("gap_seconds")\n    )\n    .select(["customer_id", "event_id", "gap_seconds"])\n    .collect()\n)\n',    'q13': 'import polars as pl\n\ncustomers = pl.scan_csv("data/customers.csv", try_parse_dates=True)\norders = pl.scan_csv("data/orders.csv", try_parse_dates=True)\nevents = pl.scan_csv("data/events.csv", try_parse_dates=True)\n\ncustomers_with_orders = (\n    orders\n    .filter(pl.col("status") == "completed")\n    .select("customer_id")\n    .unique()\n    .join(customers.select(["customer_id", "country"]), on="customer_id", how="inner")\n    .group_by("country")\n    .agg(pl.len().alias("customers_with_completed_orders"))\n)\n\ncustomers_with_events = (\n    events\n    .select("customer_id")\n    .unique()\n    .join(customers.select(["customer_id", "country"]), on="customer_id", how="inner")\n    .group_by("country")\n    .agg(pl.len().alias("customers_with_events"))\n)\n\nresult = (\n    customers_with_events\n    .join(customers_with_orders, on="country", how="left")\n    .with_columns(pl.col("customers_with_completed_orders").fill_null(0))\n    .with_columns(\n        (pl.col("customers_with_completed_orders") / pl.col("customers_with_events")).alias("conversion_rate")\n    )\n    .sort("country")\n    .collect()\n)\n',    'q14': 'import polars as pl\n\nproducts = pl.scan_csv("data/products.csv", try_parse_dates=True)\norder_items = pl.scan_csv("data/order_items.csv", try_parse_dates=True)\n\nresult = (\n    order_items\n    .join(products.select(["product_id", "category"]), on="product_id", how="inner")\n    .with_columns((pl.col("quantity") * pl.col("unit_price") - pl.col("item_discount")).alias("net_sales"))\n    .group_by(["category", "product_id"])\n    .agg(pl.col("net_sales").sum().alias("net_sales"))\n    .with_columns(\n        pl.col("net_sales").rank("dense", descending=True).over("category").alias("rank")\n    )\n    .filter(pl.col("rank") <= 3)\n    .sort(["category", "rank", "product_id"])\n    .collect()\n)\n',    'q15': 'import polars as pl\n\ncustomers = pl.scan_csv("data/customers.csv", try_parse_dates=True)\norders = pl.scan_csv("data/orders.csv", try_parse_dates=True)\n\nresult = (\n    orders\n    .filter(pl.col("status") == "completed")\n    .join(customers.select(["customer_id", "segment"]), on="customer_id", how="inner")\n    .with_columns(pl.col("order_date").dt.truncate("1mo").alias("month"))\n    .group_by(["segment", "month"])\n    .agg(\n        pl.col("amount").median().alias("median_order_amount"),\n        pl.col("payment_delay_days").quantile(0.9).alias("p90_payment_delay_days"),\n    )\n    .sort(["segment", "month"])\n    .collect()\n)\n'
}

QUESTION_BY_ID = {item["id"]: item for item in QUESTIONS}
QUESTION_TO_ID = {item["question"]: item["id"] for item in QUESTIONS}
