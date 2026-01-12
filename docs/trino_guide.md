# Trino Usage Guide

Trino is configured to run on port `8090` and connects to the Iceberg REST Catalog.

## Quick Start (CLI)

To access the Trino CLI inside the container:

```bash
docker exec -it trino trino
```

## Helper Script

We have provided a helper script for convenience:

```bash
./scripts/trino-cli.sh
```

## Sample Queries

**List Catalogs:**
```sql
SHOW CATALOGS;
```

**List Schemas in Iceberg:**
```sql
SHOW SCHEMAS FROM iceberg;
```

**List Tables in Gold Schema:**
```sql
SHOW TABLES FROM iceberg.gold;
```

**Query Data:**
```sql
SELECT * FROM iceberg.gold.telemetry LIMIT 10;
```

**Check Table Metadata:**
```sql
DESCRIBE iceberg.gold.telemetry;
```
