# Quick Start Guide

Get up and running in 5 minutes!

## Step 1: Install Dependencies

```bash
pip install -r requirements.txt
```

## Step 2: Run the Pipeline
(No database setup required - SQLite is used by default)

## Step 3: Run the Pipeline

```bash
python scripts/run_pipeline.py
```

This will:
- Download F1 data from the Ergast API (takes a few minutes)
- Clean and transform the data
- Load everything into your MySQL database

## Step 4: Run Your First Query

```bash
# See what queries are available
python scripts/run_queries.py --list

# Run a query
python scripts/run_queries.py --query kpi_summary

# Export results
python scripts/run_queries.py --query kpi_summary --export
```

## That's It! 🎉

You now have a complete F1 Red Bull analytics database running locally.

### Next Steps:
- Explore queries in `database/queries/analytical_queries.sql`
- Connect Power BI to MySQL for visualizations
- Run custom queries: `mysql -u root -p F1_RedBull_Analytics`

### Need Help?
- Check the full [README.md](README.md) for detailed documentation
- Common issues: Make sure MySQL is running and config.py has correct credentials

