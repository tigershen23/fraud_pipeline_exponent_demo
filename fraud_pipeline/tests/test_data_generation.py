"""
Test module for data_generation.py
"""

import pytest
import polars as pl
import os
import sys

# Add the parent directory to the path so we can import from fraud_pipeline
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from fraud_pipeline.src.data_generation import generate_transaction_data, save_transaction_data

def test_generate_transaction_data():
    """Test that transaction data is generated correctly."""
    # Generate a small test dataset
    num_transactions = 100
    fraud_ratio = 0.1  # 10% fraudulent transactions
    
    df = generate_transaction_data(num_transactions=num_transactions, fraud_ratio=fraud_ratio)
    
    # Check the number of transactions
    assert len(df) == num_transactions, f"Expected {num_transactions} transactions, got {len(df)}"
    
    # Check the number of fraudulent transactions
    fraud_count = df.filter(pl.col("is_flagged") == True).height
    expected_fraud = int(num_transactions * fraud_ratio)
    # Allow for small variations due to rounding
    assert abs(fraud_count - expected_fraud) <= 1, \
        f"Expected ~{expected_fraud} fraudulent transactions, got {fraud_count}"
    
    # Check that all required columns are present
    required_columns = [
        "transaction_id", "timestamp", "account_number", "transaction_type", 
        "amount", "merchant_code", "recipient_account", "is_flagged"
    ]
    for col in required_columns:
        assert col in df.columns, f"Required column {col} is missing from the generated data"
    
    # Check data types
    assert df["transaction_id"].dtype == pl.Utf8, "transaction_id should be a string"
    assert df["timestamp"].dtype == pl.Datetime, "timestamp should be a datetime"
    assert df["amount"].dtype == pl.Float64, "amount should be a float"
    assert df["is_flagged"].dtype == pl.Boolean, "is_flagged should be a boolean"

def test_save_transaction_data(tmp_path):
    """Test that transaction data is saved correctly."""
    # Generate a small test dataset
    num_transactions = 10
    df = generate_transaction_data(num_transactions=num_transactions)
    
    # Save to a temporary file
    test_csv_path = tmp_path / "test_transactions.csv"
    save_transaction_data(df, str(test_csv_path))
    
    # Check that the file exists
    assert os.path.exists(test_csv_path), f"CSV file was not created at {test_csv_path}"
    
    # Load the data back and check it matches
    loaded_df = pl.read_csv(test_csv_path)
    assert len(loaded_df) == num_transactions, "Loaded data has incorrect number of rows"
    
    # Check that all columns were saved
    for col in df.columns:
        assert col in loaded_df.columns, f"Column {col} is missing from the saved data"