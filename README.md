# Retail Data Warehouse
### End-to-end dimensional data model for a retail business  
### Modelo dimensional completo para un negocio de retail

---

## EN · Overview

A production-ready data warehouse built from scratch: synthetic data generation, a full ETL pipeline, a star schema dimensional model, and a library of analytical KPI queries — all with zero external dependencies beyond Python's standard library and SQLite.

**What this demonstrates:**
- Dimensional modeling (star schema): fact table + 4 dimension tables
- ETL pipeline design: Extract → Transform → Load with logging and data quality checks
- SQL analytical patterns: window functions, CTEs, YoY growth, margin analysis
- Clean project architecture ready to swap SQLite for PostgreSQL in production

---

## ES · Descripción

Un data warehouse construido desde cero: generación de datos sintéticos, pipeline ETL completo, modelo dimensional en estrella y consultas analíticas de KPIs — sin dependencias externas más allá de la biblioteca estándar de Python y SQLite.

**Qué demuestra este proyecto:**
- Modelado dimensional (star schema): tabla de hechos + 4 dimensiones
- Diseño de pipeline ETL: Extract → Transform → Load con logging y validación de calidad
- Patrones analíticos en SQL: window functions, CTEs, crecimiento YoY, análisis de márgenes
- Arquitectura limpia, lista para reemplazar SQLite por PostgreSQL en producción

---

## Architecture / Arquitectura

```
retail-data-warehouse/
├── data/
│   ├── generate_data.py     # Synthetic data generator (500 customers, 3K orders)
│   ├── customers.csv
│   ├── products.csv
│   ├── orders.csv
│   └── order_items.csv
├── etl/
│   └── etl_pipeline.py      # Extract → Transform → Load pipeline
├── warehouse/
│   ├── schema.sql            # Star schema DDL
│   └── retail_dw.db          # SQLite database (generated)
└── analysis/
    └── queries.sql           # 10 KPI queries
```

### Star Schema

```
                    ┌─────────────┐
                    │  dim_date   │
                    └──────┬──────┘
                           │
┌──────────────┐    ┌──────┴───────┐    ┌───────────────┐
│ dim_customer │────│  fact_sales  │────│  dim_product  │
└──────────────┘    └──────┬───────┘    └───────────────┘
                           │
                    ┌──────┴──────┐
                    │ dim_channel │
                    └─────────────┘
```

**Fact table measures:** `quantity`, `unit_price`, `unit_cost`, `discount`, `gross_revenue`, `net_revenue`, `cogs`, `gross_margin`

---

## Quick Start

```bash
# 1. Generate source data
python data/generate_data.py

# 2. Run ETL pipeline
python etl/etl_pipeline.py

# 3. Run KPI queries (any SQLite client or Python)
python -c "
import sqlite3
conn = sqlite3.connect('warehouse/retail_dw.db')
# paste any query from analysis/queries.sql
"
```

---

## Sample Results / Resultados de muestra

| KPI | Value |
|-----|-------|
| Total Net Revenue (2022–2024) | $5,674,764 |
| Overall Gross Margin | 44.2% |
| YoY Growth 2023→2024 | +4.18% |
| Top Category | Sports ($1.27M) |
| Highest Margin Category | Sports (50.4%) |

---

## KPI Queries Included / Consultas incluidas

1. Revenue overview (gross, net, COGS, margin)
2. Monthly revenue trend
3. Revenue by sales channel
4. Top 10 products by net revenue
5. Revenue by product category
6. Customer segment performance
7. Top 10 customers by lifetime value
8. Return & cancellation rate
9. Weekend vs weekday sales
10. Year-over-year growth

---

## Tech Stack

`Python 3` · `SQLite` · `SQL (window functions, CTEs)` · `CSV`  
*Swap SQLite connection string for `PostgreSQL` / `Redshift` / `BigQuery` with no other changes.*

---

*Built by [R. Indira Valentina Réquiz](https://www.linkedin.com/in/indiravalentinarequiz/) · [Portfolio](https://rainvare.github.io/portfolio/)*
