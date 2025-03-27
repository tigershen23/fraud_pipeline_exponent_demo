"""
Tests for the data_generation module.
"""

import os
import pytest
import polars as pl
from src.data_generation import DataGenerator, generate_transaction_data

def test_data_generator_init():
    """Test DataGenerator initialization."""
    generator = DataGenerator(num_records=100, num_accounts=10, fraud_ratio=0.1)
    assert generator.num_records == 100
    assert generator.num_accounts == 10
    assert generator.fraud_ratio == 0.1
    assert len(generator.account_numbers) == 10

def test_generate_normal_transaction():
    """Test generating a normal transaction."""
    generator = DataGenerator(num_records=100, num_accounts=10)
    transaction = generator.generate_normal_transaction()
    
    assert "transaction_id" in transaction
    assert "account_number" in transaction
    assert "timestamp" in transaction
    assert "transaction_type" in transaction
    assert "amount" in transaction
    assert transaction["is_fraud"] is False
    assert transaction["risk_score"] == 0.0

def test_generate_fraudulent_transaction():
    """Test generating a fraudulent transaction."""
    generator = DataGenerator(num_records=100, num_accounts=10)
    transaction = generator.generate_fraudulent_transaction()
    
    assert "transaction_id" in transaction
    assert "account_number" in transaction
    assert "timestamp" in transaction
    assert "transaction_type" in transaction
    assert "amount" in transaction
    assert transaction["is_fraud"] is True

def test_generate_data():
    """Test generating a full dataset."""
    generator = DataGenerator(num_records=100, num_accounts=10, fraud_ratio=0.1)
    df = generator.generate_data()
    
    assert isinstance(df, pl.DataFrame)
    assert len(df) == 100
    assert df["is_fraud"].sum() > 0  # Some transactions should be fraudulent
    assert df["is_fraud"].sum() <= 100 * 0.1  # At most 10% fraudulent

def test_save_data(tmp_path):
    """Test saving data to CSV."""
    generator = DataGenerator(num_records=50, num_accounts=5)
    df = generator.generate_data()
    output_path = os.path.join(tmp_path, "test_transactions.csv")
    saved_path = generator.save_data(df, output_path)
    
    assert saved_path == output_path
    assert os.path.exists(output_path)
    
    # Check that we can read it back
    df_read = pl.read_csv(output_path)
    assert len(df_read) == 50

def test_generate_transaction_data(tmp_path):
    """Test the main generate_transaction_data function."""
    output_path = os.path.join(tmp_path, "test_transactions.csv")
    csv_path = generate_transaction_data(num_records=50, output_path=output_path)
    
    assert csv_path == output_path
    assert os.path.exists(output_path)
    
    # Check that we can read it back
    df = pl.read_csv(output_path)
    assert len(df) == 50
    assert "transaction_id" in df.columns
    assert "account_number" in df.columns
    assert "timestamp" in df.columns
    assert "transaction_type" in df.columns
    assert "amount" in df.columns
    assert "is_fraud" in df.columns