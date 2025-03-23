import sys
import importlib.metadata

print(f"Python version: {sys.version}")

# Verify orchestration
import prefect
print(f"Prefect version: {prefect.__version__}")

# Verify data manipulation
import polars as pl
print(f"Polars version: {pl.__version__}")

# Verify local database
import duckdb
print(f"DuckDB version: {duckdb.__version__}")

# Verify terminal output
import rich
try:
    rich_version = rich.__version__
except AttributeError:
    rich_version = importlib.metadata.version('rich')
print(f"Rich version: {rich_version}")

# Verify visualization
import matplotlib
print(f"Matplotlib version: {matplotlib.__version__}")

# Verify data simulation
import faker
try:
    faker_version = faker.__version__
except AttributeError:
    faker_version = importlib.metadata.version('faker')
print(f"Faker version: {faker_version}")

# Verify testing
import pytest
print(f"Pytest version: {pytest.__version__}")

print("\nAll dependencies verified successfully!")