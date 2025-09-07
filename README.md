# E-commerce BI Warehouse & Dashboards (DuckDB + dbt + Metabase)

Status: **Work in progress / coming soon**. This repository documents the design and end-to-end approach now; code and dashboards are being added incrementally.

This project turns the Olist Brazilian E-commerce dataset into a production-style BI stack with tested transformations, documented metrics, and exec/ops dashboards. It is designed to demonstrate business intelligence engineering beyond visualization: data modeling, SQL transformations, data quality, automation, and maintainable analytics.

---

## Objectives

1. Build a local, reproducible analytics warehouse.
2. Model a star schema and define unambiguous KPIs.
3. Author dbt transformations with tests and lineage.
4. Add data quality gates and a scheduled build.
5. Publish executive and operational dashboards.
6. Highlight analytical SQL: cohort retention, LTV, and anomaly detection.

---

## Tech Stack

- **DuckDB** — local columnar analytics database (fast, zero configuration).
- **dbt-duckdb** — SQL transformations, tests, documentation, lineage.
- **Great Expectations** — data quality checks independent of schema tests.
- **Metabase** — open-source BI for dashboards and ad hoc exploration.
- **GitHub Actions** — daily pipeline for build, test, and validation.

---

## Data

**Brazilian E-Commerce Public Dataset by Olist (Kaggle)**

Tables used:

- `olist_orders_dataset.csv`
- `olist_order_items_dataset.csv`
- `olist_order_payments_dataset.csv`
- `olist_order_reviews_dataset.csv`
- `olist_customers_dataset.csv`
- `olist_sellers_dataset.csv`
- `olist_products_dataset.csv`
- `olist_geolocation_dataset.csv`
- `product_category_name_translation.csv`

Download from Kaggle UI, or via CLI:

```bash
pip install kaggle
mkdir -p data_raw
kaggle datasets download -d olistbr/brazilian-ecommerce -p data_raw
unzip -o data_raw/brazilian-ecommerce.zip -d data_raw
```

---

## Contact

Trustan Gabriel Price
trustanprice@gmail.com
 | LinkedIn: https://www.linkedin.com/in/trustan-price-69bb17269/
 | GitHub: https://github.com/trustanprice

