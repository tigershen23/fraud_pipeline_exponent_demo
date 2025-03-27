"""
Tests for the data_processing module.
"""

import os
import pytest
import polars as pl
import duckdb
from src.data_generation import DataGenerator
from src.data_processing import DataProcessor, process_transaction_data

@pytest.fixture
def sample_data(tmp_path):
    """Generate sample transaction data for testing."""
    # Generate sample data
    generator = DataGenerator(num_records=50, num_accounts=5, fraud_ratio=0.1)
    df = generator.generate_data()
    
    # Save to CSV
    csv_path = os.path.join(tmp_path, "test_transactions.csv")
    generator.save_data(df, csv_path)
    
    return csv_path, df

def test_data_processor_init(tmp_path):
    """Test DataProcessor initialization."""
    db_path = os.path.join(tmp_path, "test.duckdb")
    processor = DataProcessor(db_path=db_path)
    
    assert processor.db_path == db_path
    assert os.path.exists(os.path.dirname(db_path))
    assert hasattr(processor, 'conn')
    
    # Clean up
    processor.close()

def test_load_data(sample_data, tmp_path):
    """Test loading data from CSV."""
    csv_path, original_df = sample_data
    
    # Create processor
    db_path = os.path.join(tmp_path, "test.duckdb")
    processor = DataProcessor(db_path=db_path)
    
    # Load data
    df = processor.load_data(csv_path)
    
    assert isinstance(df, pl.DataFrame)
    assert len(df) == len(original_df)
    assert set(df.columns) == set(original_df.columns)
    
    # Clean up
    processor.close()

def test_clean_data(sample_data, tmp_path):
    """Test cleaning transaction data."""
    csv_path, original_df = sample_data
    
    # Create processor
    db_path = os.path.join(tmp_path, "test.duckdb")
    processor = DataProcessor(db_path=db_path)
    
    # Load data
    df = processor.load_data(csv_path)
    
    # Clean data
    cleaned_df = processor.clean_data(df)
    
    assert isinstance(cleaned_df, pl.DataFrame)
    assert len(cleaned_df) <= len(df)  # May be fewer if duplicates were removed
    assert "date" in cleaned_df.columns  # New column added
    
    # Verify all merchant fields are filled
    assert cleaned_df["merchant_name"].null_count() == 0
    assert cleaned_df["merchant_category"].null_count() == 0
    
    # Clean up
    processor.close()

def test_enrich_data(sample_data, tmp_path):
    """Test enriching transaction data."""
    csv_path, original_df = sample_data
    
    # Create processor
    db_path = os.path.join(tmp_path, "test.duckdb")
    processor = DataProcessor(db_path=db_path)
    
    # Load and clean data
    df = processor.load_data(csv_path)
    cleaned_df = processor.clean_data(df)
    
    # Enrich data
    enriched_df = processor.enrich_data(cleaned_df)
    
    assert isinstance(enriched_df, pl.DataFrame)
    assert "hour_of_day" in enriched_df.columns
    assert "day_of_week" in enriched_df.columns
    assert "recency_days" in enriched_df.columns
    assert "account_transaction_count" in enriched_df.columns
    
    # Clean up
    processor.close()

def test_load_to_duckdb(sample_data, tmp_path):
    """Test loading data into DuckDB."""
    csv_path, original_df = sample_data
    
    # Create processor
    db_path = os.path.join(tmp_path, "test.duckdb")
    processor = DataProcessor(db_path=db_path)
    
    # Load, clean, and enrich data
    df = processor.load_data(csv_path)
    cleaned_df = processor.clean_data(df)
    enriched_df = processor.enrich_data(cleaned_df)
    
    # Load to DuckDB
    success = processor.load_to_duckdb(enriched_df, table_name="test_transactions")
    
    assert success is True
    
    # Verify data is in DuckDB
    result = processor.conn.execute(
        "SELECT COUNT(*) FROM test_transactions"
    ).fetchone()
    assert result[0] == len(enriched_df)
    
    # Clean up
    processor.close()

def test_process_transaction_data(sample_data, tmp_path):
    """Test the main process_transaction_data function."""
    csv_path, _ = sample_data
    db_path = os.path.join(tmp_path, "test.duckdb")
    
    # Process data
    success = process_transaction_data(csv_path=csv_path, db_path=db_path)
    
    assert success is True
    assert os.path.exists(db_path)
    
    # Verify data is in DuckDB
    conn = duckdb.connect(db_path)
    result = conn.execute("SELECT COUNT(*) FROM transactions").fetchone()
    assert result[0] > 0
    conn.close()