"""
Test module for data_loading.py
"""

import pytest
import polars as pl
import duckdb
import os
import sys
import tempfile

# Add the parent directory to the path so we can import from fraud_pipeline
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from fraud_pipeline.src.data_generation import generate_transaction_data, save_transaction_data
from fraud_pipeline.src.data_loading import load_data_to_duckdb

@pytest.fixture
def sample_data():
    """Create a small sample dataset for testing."""
    # Generate a small test dataset
    num_transactions = 20
    df = generate_transaction_data(num_transactions=num_transactions)
    
    # Create a temporary directory and file
    with tempfile.TemporaryDirectory() as temp_dir:
        csv_path = os.path.join(temp_dir, "test_transactions.csv")
        db_path = os.path.join(temp_dir, "test_transactions.duckdb")
        
        # Save the data to CSV
        save_transaction_data(df, csv_path)
        
        yield {
            'csv_path': csv_path,
            'db_path': db_path,
            'num_transactions': num_transactions
        }

def test_load_data_to_duckdb(sample_data):
    """Test that data is loaded correctly into DuckDB."""
    # Load the data into DuckDB
    try:
        con = load_data_to_duckdb(sample_data['csv_path'], sample_data['db_path'])
        
        # Check that the connection is valid
        assert con is not None, "Connection should not be None"
        
        # Count the number of rows
        result = con.execute("SELECT COUNT(*) AS count FROM transactions").fetchone()
        count = result[0]
        
        # Check that all rows were loaded
        assert count == sample_data['num_transactions'], \
            f"Expected {sample_data['num_transactions']} rows, got {count}"
        
        # Check that the table schema is correct
        schema = con.execute("DESCRIBE transactions").fetchall()
        column_names = [col[0] for col in schema]
        
        required_columns = [
            "transaction_id", "timestamp", "account_number", "transaction_type", 
            "amount", "merchant_code", "recipient_account", "is_flagged"
        ]
        
        for col in required_columns:
            assert col in column_names, f"Required column {col} is missing from the loaded table"
        
        # Check that we can query the data
        sample = con.execute("SELECT * FROM transactions LIMIT 1").fetchone()
        assert sample is not None, "Should be able to query at least one row"
        
    finally:
        # Clean up
        if 'con' in locals():
            con.close()