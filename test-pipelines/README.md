# Test Pipelines

Example pipelines for testing and demonstrating Armillary. Each pipeline uses real data from PostgreSQL databases, REST APIs, and CSV files.

## Prerequisites

- PostgreSQL running on `localhost:5432`
- The `pagila` sample database ([github.com/devrimgunduz/pagila](https://github.com/devrimgunduz/pagila))
- The `openboard_examples` database (Horizon Analytic sample data)
- Python 3 with Polars installed (`uv pip install polars`)

## Setup

1. Copy the example environment file and edit it with your credentials:

   ```bash
   cp test-pipelines/.env.example .env
   # Edit .env with your PostgreSQL connection strings
   ```

2. Import the pipelines:

   ```bash
   armillary import test-pipelines/pagila-revenue-by-category.json
   armillary import test-pipelines/openboard-order-summary.json
   armillary import test-pipelines/cross-source-analytics.json
   ```

3. Run a pipeline:

   ```bash
   armillary run "Pagila: Revenue by Category"
   ```

## Environment Variables

Connection strings are parameterized using `{{ env:VAR_NAME }}` syntax. Set these in your `.env` file or export them in your shell:

| Variable | Description | Default |
|----------|-------------|---------|
| `PAGILA_CONNECTION` | Pagila database connection string | `postgresql://localhost:5432/pagila` |
| `OPENBOARD_CONNECTION` | Openboard database connection string | `postgresql://localhost:5432/openboard_examples` |
| `OUTPUT_CONNECTION` | Output database for sink tables | `postgresql://localhost:5432/armillary_output` |

## Pipelines

### Pagila: Revenue by Category

Simple SQL-only pipeline. Joins rental, payment, inventory, film_category, and category tables from the pagila database to compute revenue by film category.

- **Sources:** 5 PostgreSQL tables
- **Transforms:** 4 SQL joins + 1 aggregation
- **Sinks:** stdout

### Openboard: Order Summary by Region

SQL pipeline using the openboard_examples database. Joins orders, order lines, products, and customers, then branches into two aggregations.

- **Sources:** 4 PostgreSQL tables
- **Transforms:** 5 SQL (enrich, join, aggregate by region, aggregate by category)
- **Sinks:** 2 stdout

### Cross-Source Analytics

Complex pipeline combining multiple source types, both SQL and Python transforms, and multiple output formats.

- **Sources:** 2 PostgreSQL databases, 2 REST APIs (USGS earthquakes, REST Countries), 1 CSV file
- **Transforms:** 3 Python (parse earthquakes, parse countries, RFM scoring) + 4 SQL (customer profiles, enrich with economics, earthquake-country join, seismic summary, segment summary)
- **Sinks:** 4 PostgreSQL tables (with indexes), 3 CSV files, 1 Parquet file, 1 stdout

Python transforms are stored as external files using `code_dir` and `code_path`:

```
transforms/
  silver/
    usgs/parse_earthquakes.py
    geo/parse_countries.py
  gold/
    customer/rfm_scoring.py
```

## Pipeline Variables

Override variables at runtime with the `-V` flag:

```bash
armillary run "Pagila: Revenue by Category" -V "pagila_connection=postgresql://user:pass@remote:5432/pagila"
```

Or set them as environment variables in your `.env` file.

## Output Database

The cross-source pipeline writes intermediate results to PostgreSQL tables in the `armillary_output` database. Create it before running:

```bash
createdb armillary_output
```

Tables created automatically: `customer_profiles`, `enriched_customers`, `earthquake_country_matches`, `customer_rfm_scores` (with indexes).
