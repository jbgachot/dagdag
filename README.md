# DagDag

This project demonstrates a simple ETL process using [Dagster](https://dagster.io/), with data ingestion to S3, [DuckDB](https://duckdb.org/) for transformation and [PostgreSQL](https://www.postgresql.org/) for storing final results.

## Prerequisites

- Docker
- [Mise](https://mise.jdx.dev/)

## Development Setup

1. Install dependencies: `mise install`
2. Install Python dependencies: `pdm install`
3. Run the Compose: `docker compose up -d`
4. Start Dagster: `pdm run dev`
5. Explore by opening http://localhost:3000
